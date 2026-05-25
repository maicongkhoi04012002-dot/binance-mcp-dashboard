"""
binance_trader.py
─────────────────
Thực thi lệnh mua/bán trên Binance Spot (TESTNET hoặc LIVE) sử dụng API key.
Đây là module an toàn với dry-run mode mặc định (DRY_RUN=true).

Setup:
  1. Lấy API key tại: https://testnet.binance.vision  (testnet, miễn phí)
     hoặc: https://www.binance.com/en/my/settings/api-management (live)
  2. Copy .env.example → .env và điền API_KEY + API_SECRET
  3. Chạy: python binance_trader.py (test dry-run)

Dùng trong binance_feed.py:
  from binance_trader import BinanceTrader
  trader = BinanceTrader()
  await trader.execute(action="BUY", symbol="BTCUSDT", usdt_amount=20)
"""

import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import requests
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
USE_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() in ("1", "true", "yes")
DRY_RUN     = os.getenv("BINANCE_DRY_RUN", "true").lower() in ("1", "true", "yes")

# Risk management defaults (override via env)
MAX_USDT_PER_TRADE  = float(os.getenv("MAX_USDT_PER_TRADE", "20"))   # max $ per single trade
MAX_OPEN_TRADES     = int(os.getenv("MAX_OPEN_TRADES", "3"))          # max concurrent positions
STOP_LOSS_PCT       = float(os.getenv("STOP_LOSS_PCT", "2.0"))        # 2% stop loss
TAKE_PROFIT_PCT     = float(os.getenv("TAKE_PROFIT_PCT", "4.0"))      # 4% take profit

BASE_URL_LIVE    = "https://api.binance.com"
BASE_URL_TESTNET = "https://testnet.binance.vision"


# ─── ORDER RESULT ─────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    symbol:      str
    side:        str          # BUY / SELL
    qty:         float
    price:       float
    order_id:    int | None
    status:      str          # FILLED / SIMULATED / ERROR
    usdt_value:  float
    stop_loss:   float | None = None
    take_profit: float | None = None
    message:     str = ""
    raw:         dict = field(default_factory=dict)

    def __str__(self):
        icon = "[BUY]" if self.side == "BUY" else "[SELL]"
        mode = "[DRY RUN]" if self.status == "SIMULATED" else ""
        return (
            f"{icon} {mode} {self.side} {self.symbol}  "
            f"qty={self.qty:.6f}  price={self.price:.4f}  "
            f"value=${self.usdt_value:.2f}  "
            f"SL={self.stop_loss or '-'}  TP={self.take_profit or '-'}  "
            f"-> {self.status}"
        )


# ─── BINANCE TRADER ───────────────────────────────────────────────────────────

class BinanceTrader:
    """
    Safe Binance order executor with:
    - Dry-run mode (default ON — no real orders placed)
    - Testnet support
    - Risk management (max per trade, SL/TP calc)
    - Per-symbol position tracking
    """

    def __init__(self):
        self.base_url  = BASE_URL_TESTNET if USE_TESTNET else BASE_URL_LIVE
        self.dry_run   = DRY_RUN
        self.positions: dict[str, OrderResult] = {}   # symbol → current open order

        mode_str = "🧪 TESTNET" if USE_TESTNET else "🔴 LIVE"
        dry_str  = " [DRY RUN — no real orders]" if self.dry_run else " [LIVE ORDERS ENABLED]"
        log.info(f"BinanceTrader init: {mode_str}{dry_str}")

        if not API_KEY or not API_SECRET:
            log.warning("⚠️  API_KEY / API_SECRET not set — dry-run only mode forced")
            self.dry_run = True

    # ── Signing helpers ────────────────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        query = urllib.parse.urlencode(params)
        sig = hmac.new(
            API_SECRET.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = sig
        return params

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": API_KEY}

    def _get(self, path: str, params: dict = None) -> dict:
        resp = requests.get(
            self.base_url + path,
            params=self._sign(params or {}),
            headers=self._headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, params: dict) -> dict:
        resp = requests.post(
            self.base_url + path,
            params=self._sign(params),
            headers=self._headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    # ── Market info ───────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> float:
        resp = requests.get(
            f"{self.base_url}/api/v3/ticker/price",
            params={"symbol": symbol.upper()},
            timeout=8,
        )
        resp.raise_for_status()
        return float(resp.json()["price"])

    def get_step_size(self, symbol: str) -> float:
        """Get LOT_SIZE step filter for quantity rounding."""
        resp = requests.get(
            f"{self.base_url}/api/v3/exchangeInfo",
            params={"symbol": symbol.upper()},
            timeout=10,
        )
        resp.raise_for_status()
        for f in resp.json()["symbols"][0]["filters"]:
            if f["filterType"] == "LOT_SIZE":
                return float(f["stepSize"])
        return 0.000001

    def _round_qty(self, qty: float, step: float) -> float:
        if step == 0:
            return qty
        precision = len(str(step).rstrip("0").split(".")[-1])
        return round(qty - (qty % step), precision)

    def get_account_balance(self, asset: str = "USDT") -> float:
        data = self._get("/api/v3/account")
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return float(b["free"])
        return 0.0

    # ── Execute ───────────────────────────────────────────────────────────────

    async def execute(
        self,
        action:      Literal["BUY", "SELL"],
        symbol:      str,
        usdt_amount: float | None = None,
    ) -> OrderResult:
        """
        Place a market order.
        - action      : "BUY" or "SELL"
        - symbol      : e.g. "BTCUSDT"
        - usdt_amount : how much USDT to spend (BUY) or receive (SELL).
                        Defaults to MAX_USDT_PER_TRADE from env.
        Returns OrderResult.
        """
        symbol = symbol.upper()
        amount = usdt_amount or MAX_USDT_PER_TRADE

        # Guard: max open trades
        if action == "BUY" and len(self.positions) >= MAX_OPEN_TRADES:
            return OrderResult(
                symbol=symbol, side=action, qty=0, price=0,
                order_id=None, status="REJECTED", usdt_value=0,
                message=f"Max open trades ({MAX_OPEN_TRADES}) reached",
            )

        # Guard: already in position for this symbol
        if action == "BUY" and symbol in self.positions:
            return OrderResult(
                symbol=symbol, side=action, qty=0, price=0,
                order_id=None, status="REJECTED", usdt_value=0,
                message=f"Already holding position for {symbol}",
            )

        try:
            price = self.get_price(symbol)
            step  = self.get_step_size(symbol)
            qty   = self._round_qty(amount / price, step)

            # Calculate SL/TP
            if action == "BUY":
                sl = round(price * (1 - STOP_LOSS_PCT / 100), 4)
                tp = round(price * (1 + TAKE_PROFIT_PCT / 100), 4)
            else:
                sl = round(price * (1 + STOP_LOSS_PCT / 100), 4)
                tp = round(price * (1 - TAKE_PROFIT_PCT / 100), 4)

            if self.dry_run:
                result = OrderResult(
                    symbol=symbol, side=action, qty=qty, price=price,
                    order_id=None, status="SIMULATED",
                    usdt_value=round(qty * price, 4),
                    stop_loss=sl, take_profit=tp,
                    message="Dry run — no real order placed",
                )
            else:
                params = {
                    "symbol":   symbol,
                    "side":     action,
                    "type":     "MARKET",
                    "quantity": qty,
                }
                raw = self._post("/api/v3/order", params)
                filled_price = float(raw.get("fills", [{}])[0].get("price", price))
                result = OrderResult(
                    symbol=symbol, side=action, qty=qty,
                    price=filled_price,
                    order_id=raw.get("orderId"),
                    status=raw.get("status", "UNKNOWN"),
                    usdt_value=round(qty * filled_price, 4),
                    stop_loss=sl, take_profit=tp,
                    raw=raw,
                )

            # Track position
            if action == "BUY" and result.status in ("FILLED", "SIMULATED"):
                self.positions[symbol] = result
            elif action == "SELL" and symbol in self.positions:
                del self.positions[symbol]

            log.info(str(result))
            return result

        except Exception as e:
            log.error(f"Order error for {symbol}: {e}")
            return OrderResult(
                symbol=symbol, side=action, qty=0, price=0,
                order_id=None, status="ERROR", usdt_value=0,
                message=str(e),
            )

    def status_report(self) -> str:
        if not self.positions:
            return "📭 No open positions"
        lines = ["📂 Open Positions:", "─" * 50]
        for sym, o in self.positions.items():
            try:
                cur = self.get_price(sym)
                pnl_pct = ((cur - o.price) / o.price * 100) * (1 if o.side=="BUY" else -1)
                pnl_str = f"{'▲' if pnl_pct>=0 else '▼'} {pnl_pct:+.2f}%"
            except Exception:
                pnl_str = "—"
            lines.append(
                f"  {sym:<12} {o.side}  entry={o.price:.4f}  "
                f"qty={o.qty:.6f}  PnL={pnl_str}  "
                f"SL={o.stop_loss}  TP={o.take_profit}"
            )
        return "\n".join(lines)


# ─── CLI TEST ─────────────────────────────────────────────────────────────────

async def _demo():
    import asyncio
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    trader = BinanceTrader()
    print(f"\nDRY_RUN={trader.dry_run}  TESTNET={USE_TESTNET}")
    print(f"MAX_USDT_PER_TRADE=${MAX_USDT_PER_TRADE}  SL={STOP_LOSS_PCT}%  TP={TAKE_PROFIT_PCT}%\n")

    # Simulate BUY
    r = await trader.execute("BUY", "BTCUSDT", usdt_amount=20)
    print(r)
    print()
    print(trader.status_report())
    print()

    # Simulate SELL
    r2 = await trader.execute("SELL", "BTCUSDT", usdt_amount=20)
    print(r2)
    print()
    print(trader.status_report())


if __name__ == "__main__":
    import asyncio
    asyncio.run(_demo())
