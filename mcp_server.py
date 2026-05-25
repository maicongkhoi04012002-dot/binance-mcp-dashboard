"""
mcp_server.py
─────────────
MCP server exposing TradingView signals + Binance live data as tools
for AI editors (Antigravity, Cursor, Claude Desktop, etc.)

Run (stdio mode – the editor spawns this):
    python mcp_server.py

Or configure in mcp.json / cursor settings.
"""

import asyncio
import json
import os
import sys
import requests
from collections import deque

# Ensure database module is importable when spawned from any cwd
sys.path.insert(0, os.path.dirname(__file__))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from database import (
    init_db_sync,
    fetch_latest,
    fetch_active,
    fetch_stats,
    fetch_by_ticker_timeframe,
)

# ─── BINANCE REST HELPERS ────────────────────────────────────────────────────

BINANCE_REST = "https://api.binance.com/api/v3"


def _binance_get(endpoint: str, params: dict) -> dict | list:
    resp = requests.get(f"{BINANCE_REST}/{endpoint}", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _calc_rsi(closes: list, period: int = 14) -> float | None:
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


def _calc_ema(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 6)


def _calc_wma(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    weights = list(range(1, period + 1))
    subset  = closes[-period:]
    wma = sum(p * w for p, w in zip(subset, weights)) / sum(weights)
    return round(wma, 6)


def _fetch_closes(symbol: str, interval: str, limit: int = 100) -> list:
    klines = _binance_get("klines", {"symbol": symbol.upper(), "interval": interval, "limit": limit})
    return [float(k[4]) for k in klines]

# ── Initialise DB and Trader before handling any calls ───────────────────────
init_db_sync()

from binance_trader import BinanceTrader
_trader = BinanceTrader()

server = Server("tradingview-mcp")


# ─────────────────────────────────────────────────────────────────────────────
#  TOOL DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="tv_get_latest_signals",
            description=(
                "Get the most recent TradingView signals received via webhook. "
                "Optionally filter by ticker symbol."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Filter by ticker, e.g. 'BTCUSDT'. Leave empty for all tickers."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of signals to return (default 10, max 50).",
                        "default": 10
                    }
                }
            }
        ),

        types.Tool(
            name="tv_get_active_signals",
            description=(
                "Get currently active trading signals — tickers where the last received "
                "signal is BUY, SELL, LONG, or SHORT (i.e. position not yet closed)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Filter by specific ticker. Leave empty for all."
                    }
                }
            }
        ),

        types.Tool(
            name="tv_get_signal_stats",
            description=(
                "Get statistics on received signals: counts per action type (BUY/SELL/etc.) "
                "and totals. Optionally filter by ticker."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Filter by ticker. Leave empty for global stats."
                    }
                }
            }
        ),

        types.Tool(
            name="tv_get_signals_by_timeframe",
            description=(
                "Get signals for a specific ticker AND timeframe combination. "
                "Useful for multi-timeframe analysis (e.g. BTCUSDT on 1D, 4H, 1H)."
            ),
            inputSchema={
                "type": "object",
                "required": ["ticker", "timeframe"],
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Ticker symbol, e.g. 'BTCUSDT'"
                    },
                    "timeframe": {
                        "type": "string",
                        "description": "Timeframe as sent by TradingView, e.g. '1D', '240', '60', '15'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of signals to return (default 20).",
                        "default": 20
                    }
                }
            }
        ),

        types.Tool(
            name="tv_analyze_mtf_confluence",
            description=(
                "Multi-timeframe confluence analysis for a ticker. "
                "Checks the latest signal on each of the three standard timeframes "
                "(1D, 4H, 1H) and returns a confluence score and recommendation. "
                "Great for deciding high-probability entries."
            ),
            inputSchema={
                "type": "object",
                "required": ["ticker"],
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Ticker symbol, e.g. 'BTCUSDT'"
                    }
                }
            }
        ),

        # ── BINANCE LIVE DATA TOOLS ──────────────────────────────────────────
        types.Tool(
            name="binance_get_price",
            description=(
                "Get the current price and 24h statistics for a Binance symbol. "
                "Returns last price, 24h change %, high, low, volume, and quote volume. "
                "No API key required."
            ),
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Binance trading pair, e.g. 'BTCUSDT', 'ETHUSDT'"
                    }
                }
            }
        ),

        types.Tool(
            name="binance_get_indicators",
            description=(
                "Calculate RSI(14), EMA(9), and WMA(45) live from Binance kline data. "
                "Works for any symbol and timeframe without needing the feed running. "
                "Also returns the current confluence signal (BUY/SELL/NEUTRAL)."
            ),
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Trading pair, e.g. 'BTCUSDT'"
                    },
                    "interval": {
                        "type": "string",
                        "description": "Kline interval: '1h', '4h', '1d', '15m', etc. (default '1h')",
                        "default": "1h"
                    }
                }
            }
        ),

        types.Tool(
            name="binance_get_orderbook",
            description=(
                "Get the current order book (top bids and asks) for a Binance symbol. "
                "Returns top N bid/ask levels with price and quantity, plus bid/ask "
                "pressure ratio to gauge buy vs sell strength."
            ),
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Trading pair, e.g. 'BTCUSDT'"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Number of levels to show (default 5, max 20)",
                        "default": 5
                    }
                }
            }
        ),

        types.Tool(
            name="binance_execute_order",
            description=(
                "Execute a BUY or SELL market order on Binance. "
                "By default runs in DRY RUN mode (simulates without placing real orders). "
                "Requires BINANCE_API_KEY and BINANCE_API_SECRET in .env to place real orders. "
                "Includes automatic stop-loss and take-profit calculation. "
                "Use BINANCE_TESTNET=true for the Binance testnet (safe testing)."
            ),
            inputSchema={
                "type": "object",
                "required": ["action", "symbol"],
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["BUY", "SELL"],
                        "description": "Order direction: BUY or SELL"
                    },
                    "symbol": {
                        "type": "string",
                        "description": "Trading pair, e.g. 'BTCUSDT'"
                    },
                    "usdt_amount": {
                        "type": "number",
                        "description": "USDT amount to trade (default: MAX_USDT_PER_TRADE from .env)"
                    }
                }
            }
        ),

        types.Tool(
            name="binance_positions",
            description=(
                "Show current open trading positions with live P&L. "
                "Lists entry price, quantity, current price, unrealized gain/loss, "
                "and stop-loss / take-profit levels for each open trade."
            ),
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
    ]


# ─────────────────────────────────────────────────────────────────────────────
#  TOOL HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:

    # ── tv_get_latest_signals ────────────────────────────────────────────────
    if name == "tv_get_latest_signals":
        ticker = arguments.get("ticker")
        limit  = min(int(arguments.get("limit", 10)), 50)
        rows   = await fetch_latest(ticker=ticker, limit=limit)

        if not rows:
            msg = f"No signals found" + (f" for {ticker}" if ticker else "") + "."
            return [types.TextContent(type="text", text=msg)]

        lines = [f"{'#':<5} {'Time':<20} {'Ticker':<12} {'TF':<6} {'Action':<8} "
                 f"{'Price':<12} {'RSI':<8} {'EMA9':<10} {'WMA45':<10}"]
        lines.append("─" * 90)
        for r in rows:
            lines.append(
                f"{r['id']:<5} {str(r['received_at'])[:19]:<20} "
                f"{r['ticker']:<12} {str(r['timeframe'] or ''):<6} "
                f"{r['action']:<8} {str(r['price'] or ''):<12} "
                f"{str(r['rsi'] or ''):<8} {str(r['ema9'] or ''):<10} "
                f"{str(r['wma45'] or ''):<10}"
            )
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ── tv_get_active_signals ────────────────────────────────────────────────
    elif name == "tv_get_active_signals":
        ticker = arguments.get("ticker")
        rows   = await fetch_active(ticker=ticker)

        if not rows:
            return [types.TextContent(
                type="text",
                text="No active positions found. All tickers show EXIT/CLOSE or no signals."
            )]

        lines = ["🟢 ACTIVE SIGNALS\n" + "─" * 60]
        for r in rows:
            direction = "🟢 LONG" if r["action"].upper() in ("BUY", "LONG") else "🔴 SHORT"
            lines.append(
                f"{direction}  {r['ticker']}  [{r['timeframe'] or '?'}]  "
                f"@ {r['price'] or 'N/A'}  "
                f"RSI={r['rsi'] or 'N/A'}  "
                f"EMA9={r['ema9'] or 'N/A'}  "
                f"WMA45={r['wma45'] or 'N/A'}  "
                f"({str(r['received_at'])[:16]})"
            )
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ── tv_get_signal_stats ──────────────────────────────────────────────────
    elif name == "tv_get_signal_stats":
        ticker = arguments.get("ticker")
        stats  = await fetch_stats(ticker=ticker)

        scope  = f"for {ticker.upper()}" if ticker else "(all tickers)"
        lines  = [f"📊 Signal Statistics {scope}", "─" * 40]
        for k, v in stats.items():
            label = "TOTAL" if k == "_total" else k.upper()
            lines.append(f"  {label:<12}: {v}")
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ── tv_get_signals_by_timeframe ──────────────────────────────────────────
    elif name == "tv_get_signals_by_timeframe":
        ticker    = arguments["ticker"]
        timeframe = arguments["timeframe"]
        limit     = min(int(arguments.get("limit", 20)), 50)
        rows      = await fetch_by_ticker_timeframe(ticker, timeframe, limit)

        if not rows:
            return [types.TextContent(
                type="text",
                text=f"No signals for {ticker.upper()} [{timeframe}]."
            )]

        lines = [f"📈 {ticker.upper()} [{timeframe}] — last {len(rows)} signals", "─" * 60]
        for r in rows:
            lines.append(
                f"  {str(r['received_at'])[:19]}  {r['action']:<8}  "
                f"P={r['price'] or '?'}  RSI={r['rsi'] or '?'}  "
                f"EMA9={r['ema9'] or '?'}  WMA45={r['wma45'] or '?'}"
            )
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ── tv_analyze_mtf_confluence ────────────────────────────────────────────
    elif name == "tv_analyze_mtf_confluence":
        ticker = arguments["ticker"].upper()
        tf_map = {
            "1D":  ["1D", "D", "1440"],
            "4H":  ["240", "4H", "4h"],
            "1H":  ["60",  "1H", "1h"],
        }

        results = {}
        for label, candidates in tf_map.items():
            found = None
            for tf in candidates:
                rows = await fetch_by_ticker_timeframe(ticker, tf, limit=1)
                if rows:
                    found = rows[0]
                    break
            results[label] = found

        bullish_tfs = []
        bearish_tfs = []
        unknown_tfs = []

        for label, sig in results.items():
            if sig is None:
                unknown_tfs.append(label)
            elif sig["action"].upper() in ("BUY", "LONG"):
                bullish_tfs.append(label)
            elif sig["action"].upper() in ("SELL", "SHORT"):
                bearish_tfs.append(label)
            else:
                unknown_tfs.append(label)

        score = len(bullish_tfs) - len(bearish_tfs)
        if score == 3:
            recommendation = "🟢 STRONG BUY  — all 3 timeframes aligned BULLISH"
        elif score == 2:
            recommendation = "🟡 MODERATE BUY — 2/3 timeframes BULLISH"
        elif score == -3:
            recommendation = "🔴 STRONG SELL — all 3 timeframes aligned BEARISH"
        elif score == -2:
            recommendation = "🟠 MODERATE SELL — 2/3 timeframes BEARISH"
        elif score == 0 and not unknown_tfs:
            recommendation = "⚪ NEUTRAL / MIXED — conflicting signals, wait"
        else:
            recommendation = "⚠️  INSUFFICIENT DATA — some timeframes missing signals"

        lines = [
            f"🔍 MTF Confluence Analysis: {ticker}",
            "─" * 50,
            f"  1D  : {_fmt_signal(results['1D'])}",
            f"  4H  : {_fmt_signal(results['4H'])}",
            f"  1H  : {_fmt_signal(results['1H'])}",
            "",
            f"  Confluence Score : {score:+d}",
            f"  Recommendation   : {recommendation}",
        ]
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ── binance_get_price ────────────────────────────────────────────────────
    elif name == "binance_get_price":
        symbol = arguments["symbol"].upper()
        try:
            data = _binance_get("ticker/24hr", {"symbol": symbol})
            price   = float(data["lastPrice"])
            chg_pct = float(data["priceChangePercent"])
            high    = float(data["highPrice"])
            low     = float(data["lowPrice"])
            vol     = float(data["volume"])
            qvol    = float(data["quoteVolume"])
            arrow   = "📈" if chg_pct >= 0 else "📉"
            sign    = "+" if chg_pct >= 0 else ""
            lines = [
                f"💰 {symbol} — Live Price",
                "─" * 40,
                f"  Price       : {price:,.4f} USDT",
                f"  24h Change  : {arrow} {sign}{chg_pct:.2f}%",
                f"  24h High    : {high:,.4f}",
                f"  24h Low     : {low:,.4f}",
                f"  24h Volume  : {vol:,.2f} {symbol.replace('USDT','').replace('BTC','')}",
                f"  Quote Vol   : ${qvol:,.0f}",
            ]
            return [types.TextContent(type="text", text="\n".join(lines))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"❌ Error fetching price for {symbol}: {e}")]

    # ── binance_get_indicators ───────────────────────────────────────────────
    elif name == "binance_get_indicators":
        symbol   = arguments["symbol"].upper()
        interval = arguments.get("interval", "1h")
        try:
            closes = _fetch_closes(symbol, interval, limit=100)
            rsi   = _calc_rsi(closes, 14)
            ema9  = _calc_ema(closes, 9)
            wma45 = _calc_wma(closes, 45)
            price = closes[-1] if closes else None

            # Determine signal
            if rsi is not None and ema9 is not None and wma45 is not None:
                if ema9 > wma45 and rsi > 50:
                    signal = "🟢 BUY"
                elif ema9 < wma45 and rsi < 50:
                    signal = "🔴 SELL"
                else:
                    signal = "⚪ NEUTRAL"
            else:
                signal = "⚠️  INSUFFICIENT DATA"

            lines = [
                f"📊 {symbol} [{interval.upper()}] — Live Indicators",
                "─" * 45,
                f"  Close Price : {price:,.4f}" if price else "  Close Price : N/A",
                f"  RSI(14)     : {rsi}",
                f"  EMA(9)      : {ema9}",
                f"  WMA(45)     : {wma45}",
                "",
                f"  Signal      : {signal}",
                f"  Candles used: {len(closes)}",
            ]
            return [types.TextContent(type="text", text="\n".join(lines))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"❌ Error calculating indicators for {symbol} [{interval}]: {e}")]

    # ── binance_get_orderbook ────────────────────────────────────────────────
    elif name == "binance_get_orderbook":
        symbol = arguments["symbol"].upper()
        depth  = min(int(arguments.get("depth", 5)), 20)
        try:
            data = _binance_get("depth", {"symbol": symbol, "limit": depth})
            bids = data.get("bids", [])[:depth]   # [[price, qty], ...]
            asks = data.get("asks", [])[:depth]

            total_bid_qty = sum(float(b[1]) for b in bids)
            total_ask_qty = sum(float(a[1]) for a in asks)
            ratio = total_bid_qty / total_ask_qty if total_ask_qty else 0
            pressure = "🟢 BUY pressure" if ratio > 1.1 else ("🔴 SELL pressure" if ratio < 0.9 else "⚪ Balanced")

            lines = [f"📖 {symbol} Order Book (top {depth})", "─" * 50]
            lines.append(f"  {'BIDS (Buy)':<30} {'ASKS (Sell)':<30}")
            lines.append(f"  {'Price':<15} {'Qty':<15} {'Price':<15} {'Qty':<15}")
            lines.append("  " + "─" * 58)
            for i in range(max(len(bids), len(asks))):
                bp = f"{float(bids[i][0]):,.2f}" if i < len(bids) else ""
                bq = f"{float(bids[i][1]):.4f}"  if i < len(bids) else ""
                ap = f"{float(asks[i][0]):,.2f}" if i < len(asks) else ""
                aq = f"{float(asks[i][1]):.4f}"  if i < len(asks) else ""
                lines.append(f"  {bp:<15} {bq:<15} {ap:<15} {aq:<15}")
            lines += [
                "",
                f"  Total Bid Qty : {total_bid_qty:.4f}",
                f"  Total Ask Qty : {total_ask_qty:.4f}",
                f"  Bid/Ask Ratio : {ratio:.3f}  →  {pressure}",
            ]
            return [types.TextContent(type="text", text="\n".join(lines))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"❌ Error fetching order book for {symbol}: {e}")]

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


def _fmt_signal(sig: dict | None) -> str:
    if sig is None:
        return "— no signal"
    color = "🟢" if sig["action"].upper() in ("BUY", "LONG") else "🔴"
    return (
        f"{color} {sig['action']}  @ {sig['price'] or '?'}  "
        f"RSI={sig['rsi'] or '?'}  "
        f"({str(sig['received_at'])[:16]})"
    )


# ─────────────────────────────────────────────────────────────────────────────
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
