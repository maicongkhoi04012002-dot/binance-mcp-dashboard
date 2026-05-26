"""
binance_feed.py
───────────────
Kết nối Binance WebSocket, tính RSI(14) + EMA(9) + WMA(45) + MACD(12,26,9) real-time,
tự động phát hiện BUY/SELL signal, lưu vào signals.db.

Tính năng:
  - 15 streams: BTC/ETH/BNB/SOL/XRP × 1D/4H/1H
  - Điều kiện vào lệnh: RSI + EMA/WMA + MACD histogram + Volume
  - Email alert khi có signal BTC bất kỳ
  - Auto Stop-Loss: monitor giá 30 giây/lần, tự động SELL khi SL bị hit
  - Dry-run mặc định (không đặt lệnh thật trừ khi cấu hình .env)

Chạy:
    python binance_feed.py
"""

import asyncio
import json
import logging
import sys
import os
import threading
import time as _time
from collections import deque
from datetime import datetime

try:
    import winsound
    _HAS_WINSOUND = True
except ImportError:
    _HAS_WINSOUND = False   # non-Windows fallback

import requests
import websockets

sys.path.insert(0, os.path.dirname(__file__))
from database import init_db_sync, insert_signal
from binance_trader import BinanceTrader
from email_notifier import send_signal_email, send_stoploss_email

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────
WATCH_LIST = [
    # (ticker,    binance_interval, label)
    # ── BTC ────────────────────────────────────────────
    ("BTCUSDT",  "1d",  "1D"),
    ("BTCUSDT",  "4h",  "240"),
    ("BTCUSDT",  "1h",  "60"),
    # ── ETH ────────────────────────────────────────────
    ("ETHUSDT",  "1d",  "1D"),
    ("ETHUSDT",  "4h",  "240"),
    ("ETHUSDT",  "1h",  "60"),
    # ── BNB ────────────────────────────────────────────
    ("BNBUSDT",  "1d",  "1D"),
    ("BNBUSDT",  "4h",  "240"),
    ("BNBUSDT",  "1h",  "60"),
    # ── SOL ────────────────────────────────────────────
    ("SOLUSDT",  "1d",  "1D"),
    ("SOLUSDT",  "4h",  "240"),
    ("SOLUSDT",  "1h",  "60"),
    # ── XRP ────────────────────────────────────────────
    ("XRPUSDT",  "1d",  "1D"),
    ("XRPUSDT",  "4h",  "240"),
    ("XRPUSDT",  "1h",  "60"),
]


RSI_LEN    = 14
EMA_LEN    = 9
WMA_LEN    = 45
# MACD params
MACD_FAST  = 12
MACD_SLOW  = 26
MACD_SIG   = 9
# Volume filter: signal only when volume >= VOL_FACTOR × average volume
VOL_FACTOR = 1.0   # set > 1.0 to require above-average volume (e.g. 1.2)
WARMUP     = max(RSI_LEN, EMA_LEN, WMA_LEN, MACD_SLOW + MACD_SIG) + 10

# ─── STOP-LOSS CONFIG ────────────────────────────────────────────────────────
SL_ATR_LEN        = 14    # ATR period
SL_ATR_MULTIPLIER = 1.5   # ATR × multiplier for SL distance
SL_SWING_LOOKBACK = 10    # candles lookback for swing high/low
SL_BUFFER_PCT     = 0.1   # 0.1% buffer below swing low / above swing high

# Email: gửi alert cho BTC signal
EMAIL_BTC_ALERTS = True          # tắt = False
EMAIL_RECIPIENT  = os.getenv("EMAIL_RECIPIENT", "maicongkhoi04012002@gmail.com")

# Stop-Loss monitor
SL_CHECK_INTERVAL = 30           # giây kiểm tra SL một lần
AUTO_EXECUTE      = False         # True = tự động đặt lệnh (dùng BinanceTrader)
                                  # False = chỉ email cảnh báo (an toàn hơn)

# ─── CHUÔNG CẢNH BÁO ────────────────────────────────────────────────────────
ALERT_BEEP_ENABLED  = True       # True = kêu chuông khi có signal BUY/SELL
ALERT_BEEP_DURATION = 10         # giây kêu liên tục
ALERT_BEEP_FREQ_BUY  = 880       # Hz — tông cao cho BUY  (La5)
ALERT_BEEP_FREQ_SELL = 440       # Hz — tông thấp cho SELL (La4)
ALERT_BEEP_MS        = 400       # thời lượng mỗi beep (ms)

# Singleton trader (dry-run mặc định)
_trader = BinanceTrader()

BINANCE_REST = "https://api.binance.com/api/v3/klines"
BINANCE_WS   = "wss://stream.binance.com:9443/stream"

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BINANCE] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── BEEP ALERT ──────────────────────────────────────────────────────────────

def play_alert_beep(action: str):
    """
    Phát chuông cảnh báo trong ALERT_BEEP_DURATION giây.
    Chạy trong thread riêng để không block async loop.
    - BUY  → tông cao 880 Hz
    - SELL → tông thấp 440 Hz
    """
    if not ALERT_BEEP_ENABLED:
        return
    freq = ALERT_BEEP_FREQ_BUY if action == "BUY" else ALERT_BEEP_FREQ_SELL
    end  = _time.time() + ALERT_BEEP_DURATION
    log.info(f"🔔 ALERT BEEP [{action}] {freq}Hz — {ALERT_BEEP_DURATION}s")
    if _HAS_WINSOUND:
        while _time.time() < end:
            winsound.Beep(freq, ALERT_BEEP_MS)
    else:
        # fallback: dùng terminal bell
        while _time.time() < end:
            sys.stdout.write("\a")
            sys.stdout.flush()
            _time.sleep(0.5)


def trigger_beep(action: str):
    """Khởi chạy beep trong background thread (non-blocking)."""
    t = threading.Thread(target=play_alert_beep, args=(action,), daemon=True)
    t.start()


# ─── INDICATOR CALCULATIONS ───────────────────────────────────────────────────

def calc_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calc_ema(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 6)


def calc_wma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    weights = list(range(1, period + 1))
    subset  = closes[-period:]
    wma = sum(p * w for p, w in zip(subset, weights)) / sum(weights)
    return round(wma, 6)


def calc_macd(
    closes: list[float],
    fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[float | None, float | None, float | None]:
    """
    Returns (macd_line, signal_line, histogram).
    All None if insufficient data.
    """
    if len(closes) < slow + signal:
        return None, None, None

    def ema(data, period):
        k = 2 / (period + 1)
        e = sum(data[:period]) / period
        for p in data[period:]:
            e = p * k + e * (1 - k)
        return e

    macd_line = ema(closes, fast) - ema(closes, slow)

    # Build enough MACD history for signal EMA
    macd_history = []
    for i in range(slow, len(closes) + 1):
        window = closes[:i]
        macd_history.append(ema(window, fast) - ema(window, slow))

    if len(macd_history) < signal:
        return round(macd_line, 6), None, None

    sig_k   = 2 / (signal + 1)
    sig_ema = sum(macd_history[:signal]) / signal
    for v in macd_history[signal:]:
        sig_ema = v * sig_k + sig_ema * (1 - sig_k)

    hist = macd_line - sig_ema
    return round(macd_line, 6), round(sig_ema, 6), round(hist, 6)


def calc_atr(highs: list[float], lows: list[float], closes: list[float],
             period: int = 14) -> float | None:
    """Average True Range — đo volatility thực tế của thị trường."""
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return round(atr, 6)


# ─── PER-STREAM STATE ─────────────────────────────────────────────────────────

class StreamState:
    def __init__(self, symbol: str, interval: str, label: str):
        self.symbol      = symbol
        self.interval    = interval
        self.label       = label
        self.closes: deque[float]  = deque(maxlen=WARMUP + 10)
        self.highs:  deque[float]  = deque(maxlen=WARMUP + 10)
        self.lows:   deque[float]  = deque(maxlen=WARMUP + 10)
        self.volumes: deque[float] = deque(maxlen=WARMUP + 10)
        self.prev_action: str | None = None
        self.ready       = False

    def update(self, close: float, volume: float = 0.0,
               high: float = 0.0, low: float = 0.0):
        self.closes.append(close)
        self.volumes.append(volume)
        self.highs.append(high if high else close)
        self.lows.append(low   if low   else close)
        if len(self.closes) >= WARMUP:
            self.ready = True

    def indicators(self):
        cl = list(self.closes)
        rsi             = calc_rsi(cl, RSI_LEN)
        ema9            = calc_ema(cl, EMA_LEN)
        wma45           = calc_wma(cl, WMA_LEN)
        macd, sig, hist = calc_macd(cl, MACD_FAST, MACD_SLOW, MACD_SIG)
        vol_avg = (sum(self.volumes) / len(self.volumes)) if self.volumes else 0
        vol_cur = self.volumes[-1] if self.volumes else 0
        return rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg

    def calc_stoploss(self, action: str, entry_price: float) -> tuple[float, str]:
        """
        Tính Stop-Loss thông minh theo 3 phương pháp (ưu tiên lần lượt):
        1. Swing Low/High — SL dưới đáy gần nhất (BUY) / trên đỉnh gần nhất (SELL)
        2. ATR-based       — SL = entry ± (ATR × multiplier)
        3. % fallback      — SL = entry × (1 ± SL_PCT)
        Returns: (sl_price, method_name)
        """
        highs  = list(self.highs)
        lows   = list(self.lows)
        closes = list(self.closes)

        # ── Method 1: Swing High/Low ─────────────────────────────────────────
        lookback = min(SL_SWING_LOOKBACK, len(lows) - 1)
        if lookback >= 3:
            if action == "BUY":
                swing_low  = min(lows[-lookback:-1])   # exclude current candle
                sl_swing   = round(swing_low * (1 - SL_BUFFER_PCT / 100), 6)
                if sl_swing < entry_price:             # valid: SL must be below entry
                    return sl_swing, "swing_low"
            else:  # SELL
                swing_high = max(highs[-lookback:-1])
                sl_swing   = round(swing_high * (1 + SL_BUFFER_PCT / 100), 6)
                if sl_swing > entry_price:             # valid: SL must be above entry
                    return sl_swing, "swing_high"

        # ── Method 2: ATR-based ──────────────────────────────────────────────
        atr = calc_atr(highs, lows, closes, SL_ATR_LEN)
        if atr is not None:
            if action == "BUY":
                sl_atr = round(entry_price - atr * SL_ATR_MULTIPLIER, 6)
            else:
                sl_atr = round(entry_price + atr * SL_ATR_MULTIPLIER, 6)
            return sl_atr, f"atr_{SL_ATR_LEN}"

        # ── Method 3: % fallback ─────────────────────────────────────────────
        from binance_trader import STOP_LOSS_PCT
        if action == "BUY":
            sl_pct = round(entry_price * (1 - STOP_LOSS_PCT / 100), 6)
        else:
            sl_pct = round(entry_price * (1 + STOP_LOSS_PCT / 100), 6)
        return sl_pct, f"pct_{STOP_LOSS_PCT}"

    def detect_signal(self, rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg) -> str | None:
        if None in (rsi, ema9, wma45, macd, sig, hist):
            return None

        # Volume confirmation (if VOL_FACTOR > 1.0, require above-avg volume)
        vol_ok = (vol_avg == 0) or (vol_cur >= vol_avg * VOL_FACTOR)

        bullish = ema9 > wma45 and rsi > 50 and hist > 0 and vol_ok
        bearish = ema9 < wma45 and rsi < 50 and hist < 0 and vol_ok

        if bullish:
            return "BUY"
        elif bearish:
            return "SELL"
        return "NEUTRAL"





# ─── WEBSOCKET HANDLER ────────────────────────────────────────────────────────

async def seed_initial_signal(state: StreamState, last_kline: dict):
    """
    Tính và lưu signal ngay sau warmup từ nến đã đóng gần nhất.
    Giúp dashboard hiển thị dữ liệu ngay khi server khởi động
    mà không cần chờ nến mới đóng (có thể mất hàng giờ với 1D).
    """
    if not state.ready:
        return
    rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg = state.indicators()
    action = state.detect_signal(rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg)
    if action not in ("BUY", "SELL"):
        log.info(f"📊 {state.symbol} [{state.label}] seed: NEUTRAL — skipping")
        return

    close = float(last_kline[4])
    high  = float(last_kline[2])
    low_  = float(last_kline[3])
    open_ = float(last_kline[1])
    vol   = float(last_kline[5])

    sl_price, sl_method = state.calc_stoploss(action, close)
    sl_pct = round(abs(close - sl_price) / close * 100, 2)

    payload = {
        "ticker":    state.symbol,
        "timeframe": state.label,
        "action":    action,
        "price":     close,
        "close":     close,
        "open":      open_,
        "high":      high,
        "low":       low_,
        "volume":    vol,
        "rsi":       rsi,
        "ema9":      ema9,
        "wma45":     wma45,
        "macd":      macd,
        "macd_sig":  sig,
        "macd_hist": hist,
        "sl_price":  sl_price,
        "sl_method": sl_method,
        "sl_pct":    sl_pct,
        "source":    "binance_seed",
    }
    sig_id = await insert_signal(payload)
    state.prev_action = action
    arrow = "🟢 BUY" if action == "BUY" else "🔴 SELL"
    log.info(f"🌱 SEED #{sig_id}: {arrow} {state.symbol} [{state.label}] @ {close}  RSI={rsi}  SL={sl_price:.4f}")


async def handle_stream(state: StreamState):
    stream_name = f"{state.symbol.lower()}@kline_{state.interval}"
    url = f"{BINANCE_WS}?streams={stream_name}"

    # Warmup từ REST trước
    log.info(f"⏳ Fetching {WARMUP} historical candles for {state.symbol} [{state.label}]...")
    last_closed_kline = None
    try:
        resp = requests.get(BINANCE_REST, params={
            "symbol": state.symbol, "interval": state.interval, "limit": WARMUP
        }, timeout=10)
        resp.raise_for_status()
        klines = resp.json()
        for k in klines[:-1]:   # exclude last (current open candle)
            state.update(
                close  = float(k[4]),
                volume = float(k[5]),
                high   = float(k[2]),
                low    = float(k[3]),
            )
            last_closed_kline = k
        log.info(f"✅ {state.symbol} [{state.label}] warmup done — {len(state.closes)} candles loaded")
        # ── Seed signal ngay từ dữ liệu warmup ──────────────────────────────
        if last_closed_kline:
            await seed_initial_signal(state, last_closed_kline)
    except Exception as e:
        log.warning(f"Warmup failed for {state.symbol}: {e}")

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info(f"🔌 Connected: {state.symbol} [{state.label}]")
                async for raw in ws:
                    msg  = json.loads(raw)
                    data = msg.get("data", {})
                    k    = data.get("k", {})

                    if not k:
                        continue

                    close     = float(k["c"])
                    high      = float(k["h"])
                    low_      = float(k["l"])
                    volume    = float(k["v"])
                    is_closed = k["x"]          # True = nến đóng

                    if not is_closed:
                        continue                # chỉ xử lý nến đã đóng

                    state.update(close, volume, high=high, low=low_)

                    if not state.ready:
                        continue

                    rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg = state.indicators()
                    action = state.detect_signal(rsi, ema9, wma45, macd, sig, hist, vol_cur, vol_avg)

                    log.info(
                        f"📊 {state.symbol} [{state.label}] "
                        f"close={close:.4f}  RSI={rsi}  "
                        f"EMA9={ema9:.4f}  WMA45={wma45:.4f}  "
                        f"MACD_hist={hist}  → {action}"
                    )

                    # Chỉ lưu signal khi action thay đổi (tránh spam)
                    if action in ("BUY", "SELL") and action != state.prev_action:

                        # ── Tính Smart Stop-Loss ──────────────────────────────
                        sl_price, sl_method = state.calc_stoploss(action, close)
                        sl_pct = round(abs(close - sl_price) / close * 100, 2)

                        log.info(
                            f"🛡 SL [{sl_method}]: {sl_price:.6f}  "
                            f"(distance {sl_pct:.2f}% from entry)"
                        )

                        payload = {
                            "ticker":     state.symbol,
                            "timeframe":  state.label,
                            "action":     action,
                            "price":      close,
                            "close":      close,
                            "open":       float(k["o"]),
                            "high":       high,
                            "low":        low_,
                            "volume":     volume,
                            "rsi":        rsi,
                            "ema9":       ema9,
                            "wma45":      wma45,
                            "macd":       macd,
                            "macd_sig":   sig,
                            "macd_hist":  hist,
                            "sl_price":   sl_price,
                            "sl_method":  sl_method,
                            "sl_pct":     sl_pct,
                            "source":     "binance_ws",
                        }
                        sig_id = await insert_signal(payload)
                        state.prev_action = action

                        arrow = "🟢 BUY" if action == "BUY" else "🔴 SELL"
                        log.info(
                            f"SIGNAL #{sig_id}: {arrow}  "
                            f"{state.symbol} [{state.label}] @ {close}  "
                            f"SL={sl_price:.4f} ({sl_method})"
                        )

                        # ── CHUÔNG CẢNH BÁO 10 GIÂY ────────────────────────────
                        trigger_beep(action)

                        # ── AUTO EXECUTE + SL TRACK (dry-run mặc định) ──────────
                        if AUTO_EXECUTE or _trader.dry_run:
                            order = await _trader.execute(action, state.symbol, usdt_amount=None)
                            sl    = order.stop_loss
                            tp    = order.take_profit
                            log.info(f"Order: {order}")
                        else:
                            sl, tp = None, None

                        # ── EMAIL ALERT cho BTC ───────────────────────────────
                        if EMAIL_BTC_ALERTS and "BTC" in state.symbol:
                            asyncio.create_task(send_signal_email(
                                symbol     = state.symbol,
                                timeframe  = state.label,
                                action     = action,
                                price      = close,
                                rsi        = rsi,
                                ema9       = ema9,
                                wma45      = wma45,
                                macd_hist  = hist,
                                stop_loss  = sl,
                                take_profit= tp,
                                signal_id  = sig_id,
                                to         = EMAIL_RECIPIENT,
                            ))

        except Exception as e:
            log.warning(f"⚠️  {state.symbol} [{state.label}] disconnected: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


# ─── STOP-LOSS MONITOR ──────────────────────────────────────────────────────

async def stop_loss_monitor():
    """
    Background task: kiểm tra giá hiện tại mỗi SL_CHECK_INTERVAL giây.
    Nếu giá chạm Stop Loss của bất kỳ vị thế nào:
      - Tự động gọi SELL để đóng (qua _trader)
      - Gửi email cảnh báo
    """
    log.info(f"Shield SL Monitor started (check every {SL_CHECK_INTERVAL}s)")
    while True:
        await asyncio.sleep(SL_CHECK_INTERVAL)

        if not _trader.positions:
            continue

        for symbol, pos in list(_trader.positions.items()):
            if pos.stop_loss is None:
                continue
            try:
                resp = requests.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    params={"symbol": symbol}, timeout=6
                )
                cur_price = float(resp.json()["price"])

                sl_hit = (
                    pos.side == "BUY"  and cur_price <= pos.stop_loss
                ) or (
                    pos.side == "SELL" and cur_price >= pos.stop_loss
                )

                if sl_hit:
                    pnl_pct = (cur_price - pos.price) / pos.price * 100
                    if pos.side == "SELL":
                        pnl_pct *= -1

                    log.warning(
                        f"STOP LOSS HIT: {symbol}  "
                        f"entry={pos.price:.4f}  cur={cur_price:.4f}  "
                        f"SL={pos.stop_loss:.4f}  PnL={pnl_pct:+.2f}%"
                    )

                    # Close position
                    close_side = "SELL" if pos.side == "BUY" else "BUY"
                    close_order = await _trader.execute(close_side, symbol, pos.usdt_value)
                    log.info(f"SL close order: {close_order}")

                    # Email alert
                    asyncio.create_task(send_stoploss_email(
                        symbol      = symbol,
                        timeframe   = "live",
                        entry_price = pos.price,
                        exit_price  = cur_price,
                        stop_loss   = pos.stop_loss,
                        qty         = pos.qty,
                        pnl_pct     = pnl_pct,
                        to          = EMAIL_RECIPIENT,
                    ))

            except Exception as e:
                log.warning(f"SL monitor error for {symbol}: {e}")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

async def periodic_reseed(states: list, interval_hours: float = 4.0):
    """
    Mỗi interval_hours giờ, re-fetch nến gần nhất từ Binance REST
    và cập nhật signal cho tất cả streams.
    Giải quyết vấn đề Render free tier ephemeral DB (mất data khi restart).
    """
    sleep_secs = interval_hours * 3600
    while True:
        await asyncio.sleep(sleep_secs)
        log.info(f"🔄 Periodic re-seed starting ({interval_hours}h cycle)...")
        for state in states:
            try:
                resp = requests.get(BINANCE_REST, params={
                    "symbol": state.symbol, "interval": state.interval, "limit": WARMUP
                }, timeout=10)
                resp.raise_for_status()
                klines = resp.json()
                # Reset state và load lại
                state.closes.clear()
                state.highs.clear()
                state.lows.clear()
                state.volumes.clear()
                state.ready = False
                last_closed = None
                for k in klines[:-1]:
                    state.update(
                        close  = float(k[4]),
                        volume = float(k[5]),
                        high   = float(k[2]),
                        low    = float(k[3]),
                    )
                    last_closed = k
                if last_closed:
                    await seed_initial_signal(state, last_closed)
            except Exception as e:
                log.warning(f"Re-seed error {state.symbol} [{state.label}]: {e}")
        log.info("✅ Periodic re-seed done")


async def main():
    # init_db_sync() is idempotent — safe to call multiple times
    # webhook_server.py also calls it on startup, which is fine
    init_db_sync()
    log.info(f"Binance Feed started -- monitoring {len(WATCH_LIST)} streams")
    log.info(f"   Strategy: RSI({RSI_LEN}) + EMA({EMA_LEN}) + WMA({WMA_LEN}) + MACD({MACD_FAST},{MACD_SLOW},{MACD_SIG})")
    log.info(f"   Pairs: {list(set(w[0] for w in WATCH_LIST))}")
    log.info(f"   Email BTC alerts: {EMAIL_BTC_ALERTS} -> {EMAIL_RECIPIENT}")
    log.info(f"   Auto-execute orders: {AUTO_EXECUTE} | Dry-run: {_trader.dry_run}")
    log.info(f"   SL Monitor: every {SL_CHECK_INTERVAL}s")

    states = [StreamState(sym, iv, lbl) for sym, iv, lbl in WATCH_LIST]
    await asyncio.gather(
        *[handle_stream(s) for s in states],
        stop_loss_monitor(),
        periodic_reseed(states, interval_hours=4.0),
    )



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Feed stopped by user")

