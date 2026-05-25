"""
webhook_server.py
─────────────────
FastAPI server — receives TradingView webhook alerts + serves premium
real-time dashboard showing live Binance prices, MTF confluence heatmap,
and animated signal feed.

Run:
    python webhook_server.py
    → http://localhost:8765

Expose with ngrok:
    ngrok http 8765
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

import requests as req_sync
import uvicorn
import websockets
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from dotenv import load_dotenv

from database import (
    init_db_sync,
    insert_signal,
    fetch_latest,
    fetch_stats,
    fetch_active,
    fetch_by_ticker_timeframe,
)
from backtester import (
    run_full_backtest,
    optimize_params,
    fetch_ohlcv,
    run_backtest,
    DEFAULT_PARAMS,
)

# Cache backtest results to avoid repeated long fetches
_backtest_cache: dict = {}
_optimize_cache: dict = {}

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WEBHOOK] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
# Render.com injects PORT env var automatically; fall back to WEBHOOK_PORT for local use
PORT = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", "8765")))

TRACKED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
TIMEFRAMES      = ["1D", "240", "60"]

# ─── SSE broadcast queue ─────────────────────────────────────────────────────
_sse_clients: list[asyncio.Queue] = []
# ─── Price SSE queue (separate channel for real-time prices) ─────────────────
_price_clients: list[asyncio.Queue] = []
_price_cache: dict = {}   # sym -> latest ticker data


async def _broadcast(data: dict):
    dead = []
    for q in _sse_clients:
        try:
            q.put_nowait(json.dumps(data))
        except Exception:
            dead.append(q)
    for q in dead:
        _sse_clients.remove(q)


async def _broadcast_price(data: dict):
    """Push price tick to all price-SSE subscribers."""
    msg = json.dumps(data)
    dead = []
    for q in _price_clients:
        try:
            q.put_nowait(msg)
        except Exception:
            dead.append(q)
    for q in dead:
        _price_clients.remove(q)


async def _binance_price_ws():
    """
    Server-side Binance WebSocket — connects to Binance stream and
    pushes real-time prices to browser clients via SSE /price-events.
    Runs as a background task in the FastAPI lifespan.
    """
    streams = "/".join(s.lower() + "@ticker" for s in TRACKED_SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            log.info("Connecting to Binance price WebSocket...")
            async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
                log.info("Binance price WS connected — real-time prices active")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        d   = msg.get("data", msg)
                        sym = d.get("s", "").upper()
                        if not sym:
                            continue
                        tick = {
                            "type":   "price",
                            "sym":    sym,
                            "price":  float(d.get("c", 0)),
                            "change": float(d.get("P", 0)),
                            "high":   float(d.get("h", 0)),
                            "low":    float(d.get("l", 0)),
                            "volume": float(d.get("q", 0)),
                        }
                        _price_cache[sym] = tick
                        await _broadcast_price(tick)
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"Binance price WS error: {e} — retry in 5s")
            await asyncio.sleep(5)


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_sync()
    # Start Binance price WebSocket proxy in background
    asyncio.create_task(_binance_price_ws())
    # Start Binance signal feed in background (runs alongside the web server)
    # This allows a single-process deploy on Render free tier
    try:
        import binance_feed
        asyncio.create_task(binance_feed.main())
        log.info("✅ Binance Feed started in background")
    except Exception as e:
        log.warning(f"⚠️  Could not start binance_feed: {e}")
    log.info(f"✅ Dashboard server started → http://0.0.0.0:{PORT}")
    yield


app = FastAPI(
    title="Binance Signal Dashboard",
    description="Real-time Binance signal dashboard with MCP integration",
    version="2.0.0",
    lifespan=lifespan,
)


# ─── WEBHOOK ──────────────────────────────────────────────────────────────────

@app.post("/webhook")
async def receive_webhook(request: Request):
    if WEBHOOK_SECRET:
        token = request.headers.get("X-TV-Secret", "")
        if token != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        body    = await request.body()
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not payload.get("action"):
        raise HTTPException(status_code=400, detail="'action' field is required")

    signal_id = await insert_signal(payload)
    log.info(
        f"📡 Signal #{signal_id}: {payload.get('ticker')} "
        f"{payload.get('action')} @ {payload.get('price')}"
    )

    await _broadcast({"type": "new_signal", "id": signal_id, **payload})
    return {"ok": True, "id": signal_id}


# ─── SSE ENDPOINT ─────────────────────────────────────────────────────────────

@app.get("/events")
async def sse_events(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    _sse_clients.append(q)

    async def generator():
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"   # keepalive
        finally:
            if q in _sse_clients:
                _sse_clients.remove(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/price-events")
async def price_events(request: Request):
    """SSE stream for real-time price ticks from server-side Binance WS proxy."""
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _price_clients.append(q)

    # Send cached prices immediately on connect
    async def generator():
        try:
            # Send current cached prices right away
            for sym, tick in _price_cache.items():
                yield f"data: {json.dumps(tick)}\n\n"
            yield "data: {\"type\":\"price_connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            if q in _price_clients:
                _price_clients.remove(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )



@app.get("/api/signals")
async def api_signals(ticker: str = None, limit: int = 30):
    return await fetch_latest(ticker=ticker, limit=limit)


@app.get("/api/stats")
async def api_stats():
    return await fetch_stats()


@app.get("/api/active")
async def api_active():
    return await fetch_active()


@app.get("/api/confluence")
async def api_confluence():
    """MTF confluence per symbol."""
    result = {}
    tf_aliases = {
        "1D":  ["1D", "D", "1440"],
        "4H":  ["240", "4H"],
        "1H":  ["60",  "1H"],
    }
    for sym in TRACKED_SYMBOLS:
        result[sym] = {}
        for label, aliases in tf_aliases.items():
            sig = None
            for tf in aliases:
                rows = await fetch_by_ticker_timeframe(sym, tf, limit=1)
                if rows:
                    sig = rows[0]
                    break
            result[sym][label] = {
                "action":  sig["action"]  if sig else None,
                "price":   sig["price"]   if sig else None,
                "rsi":     sig["rsi"]     if sig else None,
                "time":    str(sig["received_at"])[:16] if sig else None,
            }
    return result


@app.get("/api/prices")
async def api_prices():
    """Fetch live prices — Binance first, CoinGecko fallback if blocked."""

    # ── Try Binance main ──────────────────────────────────────────────────
    try:
        joined = "[" + ",".join(f'"{s}"' for s in TRACKED_SYMBOLS) + "]"
        resp = req_sync.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbols": joined},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            item["symbol"]: {
                "price":   float(item["lastPrice"]),
                "change":  float(item["priceChangePercent"]),
                "high":    float(item["highPrice"]),
                "low":     float(item["lowPrice"]),
                "volume":  float(item["quoteVolume"]),
                "source":  "binance",
            }
            for item in data
        }
    except Exception as e:
        log.warning(f"Binance API blocked/timeout: {e} — trying CoinGecko...")

    # ── Fallback: CoinGecko (free, no VPN needed) ─────────────────────────
    try:
        CG_IDS = {
            "BTCUSDT": "bitcoin",
            "ETHUSDT": "ethereum",
            "BNBUSDT": "binancecoin",
            "SOLUSDT": "solana",
            "XRPUSDT": "ripple",
        }
        ids = ",".join(CG_IDS.values())
        resp = req_sync.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ids,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true",
                "include_high_24h": "true",
                "include_low_24h": "true",
            },
            timeout=8,
        )
        resp.raise_for_status()
        cg = resp.json()
        result = {}
        for sym, cg_id in CG_IDS.items():
            d = cg.get(cg_id, {})
            result[sym] = {
                "price":   d.get("usd", 0),
                "change":  d.get("usd_24h_change", 0),
                "high":    d.get("usd_24h_high", 0),
                "low":     d.get("usd_24h_low", 0),
                "volume":  d.get("usd_24h_vol", 0),
                "source":  "coingecko",
            }
        log.info("Prices loaded from CoinGecko fallback")
        return result
    except Exception as e2:
        log.warning(f"CoinGecko also failed: {e2}")
        return {"error": f"All price sources failed: {e2}"}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/admin/clear-signals")
async def clear_signals():
    """Xóa toàn bộ signals trong DB (dùng để reset test data)."""
    import aiosqlite
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM signals")
        await db.commit()
        cur = await db.execute("SELECT changes()")
        row = await cur.fetchone()
        deleted = row[0] if row else 0
    log.info(f"🗑  Cleared {deleted} signals from DB")
    return {"ok": True, "deleted": deleted}


# ─── BACKTEST API ─────────────────────────────────────────────────────────────

@app.get("/api/backtest")
async def api_backtest(
    n_candles: int = 300,
    rsi_len: int = 14,
    ema_len: int = 9,
    wma_len: int = 45,
    tp_pct: float = 3.0,
    sl_pct: float = 1.5,
    vol_factor: float = 1.0,
    rsi_bull: int = 50,
    rsi_bear: int = 50,
    force: bool = False,
):
    """Run full backtest across all pairs. Results are cached."""
    params = {
        **DEFAULT_PARAMS,
        "rsi_len":    rsi_len,
        "ema_len":    ema_len,
        "wma_len":    wma_len,
        "tp_pct":     tp_pct,
        "sl_pct":     sl_pct,
        "vol_factor": vol_factor,
        "rsi_bull":   rsi_bull,
        "rsi_bear":   rsi_bear,
    }
    cache_key = f"{n_candles}_{json.dumps(params, sort_keys=True)}"

    if not force and cache_key in _backtest_cache:
        log.info("Returning cached backtest result")
        return _backtest_cache[cache_key]

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: run_full_backtest(n_candles=n_candles, params=params)
        )
        _backtest_cache[cache_key] = result
        return result
    except Exception as e:
        log.error(f"Backtest error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/pair")
async def api_backtest_pair(
    symbol: str = "BTCUSDT",
    interval: str = "4h",
    timeframe: str = "4H",
    n_candles: int = 500,
):
    """Backtest a single pair with default params."""
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None, lambda: fetch_ohlcv(symbol.upper(), interval, limit=n_candles)
        )
        result = await loop.run_in_executor(
            None, lambda: run_backtest(candles, DEFAULT_PARAMS, symbol.upper(), timeframe)
        )
        return result
    except Exception as e:
        log.error(f"Single backtest error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/optimize")
async def api_optimize(
    symbol: str = "BTCUSDT",
    interval: str = "4h",
    timeframe: str = "4H",
    n_candles: int = 500,
    metric: str = "sharpe",
    force: bool = False,
):
    """Grid search to find optimal strategy parameters."""
    cache_key = f"opt_{symbol}_{interval}_{n_candles}_{metric}"

    if not force and cache_key in _optimize_cache:
        return _optimize_cache[cache_key]

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None, lambda: fetch_ohlcv(symbol.upper(), interval, limit=n_candles)
        )
        result = await loop.run_in_executor(
            None, lambda: optimize_params(candles, symbol.upper(), timeframe, metric=metric)
        )
        _optimize_cache[cache_key] = result
        return result
    except Exception as e:
        log.error(f"Optimize error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── PREMIUM DASHBOARD ────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>📡 Binance MCP Signal Dashboard</title>
  <meta name="description" content="Real-time Binance trading signals powered by RSI, EMA, WMA & MACD analysis via MCP"/>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
  <style>
    :root {
      --bg:        #080c14;
      --surface:   #0d1421;
      --glass:     rgba(255,255,255,0.04);
      --border:    rgba(255,255,255,0.08);
      --accent:    #3b82f6;
      --buy:       #10b981;
      --sell:      #ef4444;
      --neutral:   #6b7280;
      --text:      #e2e8f0;
      --muted:     #64748b;
      --gold:      #f59e0b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html { scroll-behavior: smooth; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      min-height: 100vh;
      overflow-x: hidden;
    }

    /* ── Background grid ── */
    body::before {
      content: '';
      position: fixed; inset: 0;
      background-image:
        linear-gradient(rgba(59,130,246,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(59,130,246,0.03) 1px, transparent 1px);
      background-size: 40px 40px;
      pointer-events: none; z-index: 0;
    }

    /* ── HEADER ── */
    header {
      position: sticky; top: 0; z-index: 100;
      background: rgba(8,12,20,0.85);
      backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--border);
      padding: 14px 28px;
      display: flex; align-items: center; gap: 16px;
    }
    .logo { font-size: 20px; font-weight: 700; color: var(--accent); letter-spacing: -.5px; }
    .logo span { color: var(--text); }
    .live-dot {
      width: 8px; height: 8px; border-radius: 50%;
      background: var(--buy);
      animation: pulse 2s infinite;
      flex-shrink: 0;
    }
    @keyframes pulse {
      0%,100% { box-shadow: 0 0 0 0 rgba(16,185,129,.6); }
      50%      { box-shadow: 0 0 0 6px rgba(16,185,129,0); }
    }
    .header-status { font-size: 12px; color: var(--muted); margin-left: auto; }
    .header-time   { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--muted); }

    /* ── LAYOUT ── */
    .page { position: relative; z-index: 1; padding: 24px 28px; max-width: 1600px; margin: 0 auto; }

    /* ── PRICE TICKER ── */
    .ticker-bar {
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 12px;
      margin-bottom: 24px;
    }
    .ticker-card {
      background: var(--glass);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      backdrop-filter: blur(8px);
      transition: border-color .2s, transform .2s;
      cursor: default;
    }
    .ticker-card:hover { border-color: var(--accent); transform: translateY(-2px); }
    .ticker-symbol { font-size: 11px; font-weight: 600; color: var(--muted); letter-spacing: 1px; margin-bottom: 4px; }
    .ticker-price  { font-size: 20px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
    .ticker-change { font-size: 12px; font-weight: 500; margin-top: 3px; }
    .ticker-vol    { font-size: 11px; color: var(--muted); margin-top: 2px; }
    .up   { color: var(--buy); }
    .down { color: var(--sell); }

    /* ── SECTION TITLE ── */
    .section-title {
      font-size: 13px; font-weight: 600; color: var(--muted);
      text-transform: uppercase; letter-spacing: 1.5px;
      margin-bottom: 12px; display: flex; align-items: center; gap: 8px;
    }
    .section-title::after {
      content: ''; flex: 1; height: 1px; background: var(--border);
    }

    /* ── MTF CONFLUENCE ── */
    .confluence-grid {
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 12px;
      margin-bottom: 28px;
    }
    .cf-card {
      background: var(--glass);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      backdrop-filter: blur(8px);
    }
    .cf-sym  { font-size: 12px; font-weight: 700; color: var(--accent); margin-bottom: 10px; }
    .cf-row  { display: flex; align-items: center; justify-content: space-between;
               font-size: 11px; padding: 4px 0; border-bottom: 1px solid var(--border); }
    .cf-row:last-child { border-bottom: none; }
    .cf-tf   { color: var(--muted); font-weight: 500; width: 30px; }
    .cf-action {
      font-size: 10px; font-weight: 700; letter-spacing: .5px;
      padding: 2px 7px; border-radius: 4px;
    }
    .cf-buy   { background: rgba(16,185,129,.15); color: var(--buy); }
    .cf-sell  { background: rgba(239,68,68,.15);  color: var(--sell); }
    .cf-none  { background: rgba(107,114,128,.1); color: var(--neutral); }
    .cf-score {
      margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--border);
      font-size: 10px; font-weight: 600; text-align: center; letter-spacing: .5px;
    }

    /* ── SIGNAL TABLE ── */
    .table-wrap {
      background: var(--glass);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      backdrop-filter: blur(8px);
    }
    .table-header {
      display: flex; align-items: center; justify-content: space-between;
      padding: 16px 20px;
      border-bottom: 1px solid var(--border);
    }
    .table-title { font-size: 14px; font-weight: 600; }
    .stats-row   { display: flex; gap: 10px; }
    .stat-pill {
      font-size: 11px; padding: 4px 10px;
      border: 1px solid var(--border);
      border-radius: 20px; color: var(--muted);
    }
    .stat-pill b { color: var(--text); }

    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th {
      background: rgba(255,255,255,0.02);
      color: var(--muted); font-weight: 500;
      padding: 10px 16px; text-align: left;
      border-bottom: 1px solid var(--border);
      font-size: 11px; letter-spacing: .5px;
    }
    td { padding: 10px 16px; border-bottom: 1px solid rgba(255,255,255,0.04); }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .tr-new { animation: rowFlash 1.5s ease-out; }
    @keyframes rowFlash {
      0%   { background: rgba(59,130,246,0.2); }
      100% { background: transparent; }
    }

    .badge-action {
      display: inline-block; padding: 3px 9px; border-radius: 5px;
      font-size: 10px; font-weight: 700; letter-spacing: .8px;
    }
    .badge-buy     { background: rgba(16,185,129,.15); color: var(--buy); }
    .badge-sell    { background: rgba(239,68,68,.15);  color: var(--sell); }
    .badge-neutral { background: rgba(107,114,128,.1); color: var(--neutral); }

    .mono { font-family: 'JetBrains Mono', monospace; }

    /* ── ALERT TOAST ── */
    .alert-toast {
      position: fixed; bottom: 24px; right: 24px; z-index: 999;
      background: linear-gradient(135deg, rgba(13,20,33,.97), rgba(20,30,50,.97));
      border: 1px solid var(--accent);
      border-radius: 14px;
      padding: 16px 22px;
      min-width: 280px;
      box-shadow: 0 8px 32px rgba(0,0,0,.5), 0 0 0 1px rgba(59,130,246,.2);
      display: flex; align-items: center; gap: 14px;
      animation: toastIn .35s cubic-bezier(.16,1,.3,1);
      backdrop-filter: blur(16px);
    }
    .alert-toast.buy  { border-color: var(--buy);  box-shadow: 0 8px 32px rgba(0,0,0,.5), 0 0 16px rgba(16,185,129,.2); }
    .alert-toast.sell { border-color: var(--sell); box-shadow: 0 8px 32px rgba(0,0,0,.5), 0 0 16px rgba(239,68,68,.2); }
    .toast-icon { font-size: 28px; flex-shrink: 0; }
    .toast-body { flex: 1; }
    .toast-title { font-size: 13px; font-weight: 700; margin-bottom: 3px; }
    .toast-sub   { font-size: 11px; color: var(--muted); }
    .toast-close { cursor: pointer; color: var(--muted); font-size: 18px; line-height: 1;
                   padding: 4px; border-radius: 4px; flex-shrink: 0; }
    .toast-close:hover { color: var(--text); background: rgba(255,255,255,.08); }
    .toast-bar {
      position: absolute; bottom: 0; left: 0;
      height: 3px; border-radius: 0 0 14px 14px;
      animation: toastBar 10s linear forwards;
    }
    .buy  .toast-bar { background: var(--buy); }
    .sell .toast-bar { background: var(--sell); }
    @keyframes toastIn {
      from { transform: translateX(110%); opacity: 0; }
      to   { transform: translateX(0);    opacity: 1; }
    }
    @keyframes toastOut {
      from { transform: translateX(0);    opacity: 1; }
      to   { transform: translateX(110%); opacity: 0; }
    }
    @keyframes toastBar {
      from { width: 100%; }
      to   { width: 0%; }
    }

    /* ── FOOTER ── */
    footer {
      text-align: center; padding: 24px; font-size: 11px; color: var(--muted);
      border-top: 1px solid var(--border); margin-top: 28px;
    }

    /* ── RESPONSIVE ── */
    @media (max-width: 1200px) {
      .ticker-bar, .confluence-grid { grid-template-columns: repeat(3, 1fr); }
    }
    @media (max-width: 768px) {
      .ticker-bar, .confluence-grid { grid-template-columns: repeat(2, 1fr); }
      .page { padding: 16px; }
    }
  </style>
</head>
<body>

<header>
  <div class="logo">📡 Binance<span>MCP</span></div>
  <div class="live-dot"></div>
  <div class="header-status" id="hdr-status">Connecting…</div>
  <span id="price-src" style="font-size:11px;margin-left:8px;color:var(--muted)">loading prices…</span>
  <div class="header-time" id="hdr-time"></div>
</header>

<div class="page">

  <!-- ── PRICE TICKER ── -->
  <div class="section-title">Live Prices</div>
  <div class="ticker-bar" id="ticker-bar">
    <div class="ticker-card" id="tc-BTCUSDT"><div class="ticker-symbol">BTC/USDT</div><div class="ticker-price">—</div></div>
    <div class="ticker-card" id="tc-ETHUSDT"><div class="ticker-symbol">ETH/USDT</div><div class="ticker-price">—</div></div>
    <div class="ticker-card" id="tc-BNBUSDT"><div class="ticker-symbol">BNB/USDT</div><div class="ticker-price">—</div></div>
    <div class="ticker-card" id="tc-SOLUSDT"><div class="ticker-symbol">SOL/USDT</div><div class="ticker-price">—</div></div>
    <div class="ticker-card" id="tc-XRPUSDT"><div class="ticker-symbol">XRP/USDT</div><div class="ticker-price">—</div></div>
  </div>

  <!-- ── MTF CONFLUENCE ── -->
  <div class="section-title">MTF Confluence</div>
  <div class="confluence-grid" id="confluence-grid">
    <!-- filled by JS -->
  </div>

  <!-- ── SIGNAL TABLE ── -->
  <div class="table-wrap">
    <div class="table-header">
      <div class="table-title">📋 Recent Signals</div>
      <div class="stats-row" id="stats-row"></div>
    </div>
    <table>
      <thead>
        <tr>
          <th>#</th><th>Time (UTC)</th><th>Ticker</th><th>TF</th>
          <th>Action</th><th>Price</th><th>RSI</th>
          <th>EMA9</th><th>WMA45</th><th>MACD hist</th>
          <th>Stop Loss</th><th>SL %</th><th>Source</th>
        </tr>
      </thead>
      <tbody id="signal-tbody"></tbody>
    </table>
  </div>

</div>

<footer>
  Binance MCP Dashboard · Data from Binance Public API · MCP: python mcp_server.py &nbsp;|&nbsp;
  Refresh prices every 10s · Signals pushed via SSE
</footer>

<script>
const SYMBOLS = ['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT'];
const TFS     = ['1D','4H','1H'];

/* ── Clock ── */
function tickClock() {
  const d = new Date();
  document.getElementById('hdr-time').textContent =
    d.toUTCString().replace('GMT','UTC');
}
setInterval(tickClock, 1000); tickClock();

/* ── Price Ticker — Binance WebSocket realtime (fallback: CoinGecko via server) ── */
const _priceData = {};   // cache: sym -> {price,change,high,low,volume}

function renderTicker(sym, info) {
  const card = document.getElementById('tc-' + sym);
  if (!card) return;
  const up   = (info.change || 0) >= 0;
  const base = sym.replace('USDT','');
  const prev = _priceData[sym];
  const flash = prev && prev.price !== info.price
    ? (info.price > prev.price ? 'style="color:var(--buy)"' : 'style="color:var(--sell)"') : '';
  _priceData[sym] = info;
  card.innerHTML = `
    <div class="ticker-symbol">${base}/USDT <span style="font-size:9px;color:var(--muted);letter-spacing:.5px">${info.src||''}</span></div>
    <div class="ticker-price" ${flash}>${Number(info.price).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4})}</div>
    <div class="ticker-change ${up?'up':'down'}">${up?'▲':'▼'} ${Math.abs(info.change||0).toFixed(2)}%</div>
    <div class="ticker-vol">Vol $${((info.volume||0)/1e6).toFixed(1)}M</div>`;
}

function connectPriceWS() {
  // Connect to server-side price SSE (server proxies Binance WS \u2014 works even if network blocks Binance)
  const es = new EventSource('/price-events');

  es.onopen = () => {
    document.getElementById('price-src').textContent = '\u26a1 Real-time';
    document.getElementById('price-src').style.color  = 'var(--buy)';
  };

  es.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      if (d.type === 'price_connected' || d.type === 'price') {
        if (d.sym) renderTicker(d.sym, { ...d, src: 'LIVE' });
      }
    } catch(_) {}
  };

  es.onerror = () => {
    document.getElementById('price-src').textContent = '\ud83d\udfe1 polling';
    document.getElementById('price-src').style.color  = 'var(--gold)';
    // Fallback: REST every 15s
    setTimeout(() => {
      refreshPricesREST();
      setInterval(refreshPricesREST, 15_000);
    }, 3000);
    es.close();
  };
}

async function refreshPricesREST() {
  try {
    const data = await fetch('/api/prices').then(r => r.json());
    for (const [sym, info] of Object.entries(data)) {
      if (info.error) continue;
      renderTicker(sym, { ...info, src: info.source === 'coingecko' ? 'CG' : '' });
    }
  } catch(e) {}
}

/* ── Confluence Grid ── */
async function refreshConfluence() {
  try {
    const data = await fetch('/api/confluence').then(r => r.json());
    const grid = document.getElementById('confluence-grid');
    grid.innerHTML = '';
    for (const sym of SYMBOLS) {
      const tfs = data[sym] || {};
      let bull = 0, bear = 0;
      const rows = ['1D','4H','1H'].map(tf => {
        const s = tfs[tf];
        let cls = 'cf-none', lbl = '—';
        if (s && s.action) {
          const a = s.action.toUpperCase();
          if (['BUY','LONG'].includes(a))  { cls='cf-buy';  lbl='BUY';  bull++; }
          else if (['SELL','SHORT'].includes(a)) { cls='cf-sell'; lbl='SELL'; bear++; }
          else { lbl = a; }
        }
        const rsi = s && s.rsi ? `<span style="color:var(--muted)">RSI ${s.rsi}</span>` : '';
        return `<div class="cf-row">
          <span class="cf-tf">${tf}</span>
          ${rsi}
          <span class="cf-action ${cls}">${lbl}</span>
        </div>`;
      }).join('');

      const score = bull - bear;
      let scoreColor = 'var(--neutral)', scoreText = 'MIXED';
      if (score === 3)  { scoreColor='var(--buy)';  scoreText='🟢 STRONG BUY'; }
      else if (score === 2) { scoreColor='var(--buy)';  scoreText='🟡 BUY 2/3'; }
      else if (score === -3){ scoreColor='var(--sell)'; scoreText='🔴 STRONG SELL'; }
      else if (score === -2){ scoreColor='var(--sell)'; scoreText='🟠 SELL 2/3'; }

      const base = sym.replace('USDT','');
      grid.innerHTML += `<div class="cf-card">
        <div class="cf-sym">${base}</div>
        ${rows}
        <div class="cf-score" style="color:${scoreColor}">${scoreText}</div>
      </div>`;
    }
  } catch(e) {}
}

/* ── Signal Table ── */
function actionClass(a) {
  if (!a) return 'badge-neutral';
  const u = a.toUpperCase();
  if (['BUY','LONG'].includes(u))  return 'badge-buy';
  if (['SELL','SHORT'].includes(u)) return 'badge-sell';
  return 'badge-neutral';
}

function buildRow(s, isNew) {
  const ac  = actionClass(s.action);
  const time = (s.received_at||'').substring(0,19).replace('T',' ');
  let extra = {};
  try { extra = s.extra ? JSON.parse(s.extra) : {}; } catch(e) {}
  const macdH    = extra.macd_hist ?? s.macd_hist ?? '—';
  const slPrice  = extra.sl_price  ?? s.sl_price  ?? null;
  const slMethod = extra.sl_method ?? s.sl_method ?? null;
  const slPct    = extra.sl_pct    ?? s.sl_pct    ?? null;

  const isLong  = (s.action||'').toUpperCase() === 'BUY';
  const slColor = slPrice ? (isLong ? 'var(--sell)' : 'var(--gold)') : 'var(--muted)';
  const slLabel = slPrice
    ? `<span style="color:${slColor};font-family:'JetBrains Mono',monospace">${Number(slPrice).toLocaleString('en-US',{maximumFractionDigits:6})}</span>
       ${slMethod ? `<br><span style="font-size:9px;color:var(--muted);letter-spacing:.5px">${slMethod.toUpperCase()}</span>` : ''}`
    : '<span style="color:var(--muted)">—</span>';
  const slPctLabel = slPct
    ? `<span style="color:${slColor}">−${slPct}%</span>`
    : '<span style="color:var(--muted)">—</span>';

  return `<tr class="${isNew?'tr-new':''}">
    <td class="mono" style="color:var(--muted)">${s.id}</td>
    <td class="mono" style="color:var(--muted);font-size:11px">${time}</td>
    <td><b>${s.ticker||'—'}</b></td>
    <td style="color:var(--muted)">${s.timeframe||'—'}</td>
    <td><span class="badge-action ${ac}">${s.action||'—'}</span></td>
    <td class="mono">${s.price ? Number(s.price).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4}) : '—'}</td>
    <td class="mono ${(s.rsi&&s.rsi>70)?'down':(s.rsi&&s.rsi<30)?'up':''}">${s.rsi??'—'}</td>
    <td class="mono">${s.ema9??'—'}</td>
    <td class="mono">${s.wma45??'—'}</td>
    <td class="mono" style="color:${macdH>0?'var(--buy)':macdH<0?'var(--sell)':'var(--muted)'}">${macdH}</td>
    <td style="line-height:1.6">${slLabel}</td>
    <td>${slPctLabel}</td>
    <td style="color:var(--muted);font-size:10px">${extra.source||s.source||'tv_webhook'}</td>
  </tr>`;
}


async function refreshSignals() {
  try {
    const [signals, stats] = await Promise.all([
      fetch('/api/signals?limit=40').then(r=>r.json()),
      fetch('/api/stats').then(r=>r.json()),
    ]);

    document.getElementById('signal-tbody').innerHTML =
      signals.map(s => buildRow(s, false)).join('');

    const pills = Object.entries(stats)
      .map(([k,v]) => `<div class="stat-pill">${k==='_total'?'Total':k}: <b>${v}</b></div>`)
      .join('');
    document.getElementById('stats-row').innerHTML = pills;
  } catch(e) {}
}

/* ── Web Audio Alert ── */
let _audioCtx = null;
let _beepHandle = null;

function getAudioCtx() {
  if (!_audioCtx || _audioCtx.state === 'closed') {
    _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (_audioCtx.state === 'suspended') _audioCtx.resume();
  return _audioCtx;
}

function playSignalAlert(action) {
  stopSignalAlert();
  const freq    = action.toUpperCase() === 'BUY' ? 880 : 440;
  const ctx     = getAudioCtx();
  const endTime = ctx.currentTime + 10;   // 10 giây
  let   t       = ctx.currentTime;
  const beepDur = 0.35, gapDur = 0.08;

  // Phát chuỗi beep liên tiếp trong 10s
  const scheduledOscs = [];
  while (t < endTime) {
    const osc  = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type      = 'sine';
    osc.frequency.setValueAtTime(freq, t);
    gain.gain.setValueAtTime(0, t);
    gain.gain.linearRampToValueAtTime(0.35, t + 0.02);
    gain.gain.setValueAtTime(0.35, t + beepDur - 0.03);
    gain.gain.linearRampToValueAtTime(0, t + beepDur);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(t);
    osc.stop(t + beepDur);
    scheduledOscs.push(osc);
    t += beepDur + gapDur;
  }
  _beepHandle = scheduledOscs;
}

function stopSignalAlert() {
  if (_beepHandle) {
    _beepHandle.forEach(o => { try { o.stop(); } catch(e) {} });
    _beepHandle = null;
  }
}

function showAlertToast(action, ticker, tf, price, slPrice) {
  // Xóa toast cũ nếu có
  document.querySelectorAll('.alert-toast').forEach(el => el.remove());

  const isBuy  = action.toUpperCase() === 'BUY';
  const icon   = isBuy ? '🟢' : '🔴';
  const label  = isBuy ? 'BUY SIGNAL' : 'SELL SIGNAL';
  const color  = isBuy ? 'var(--buy)' : 'var(--sell)';
  const base   = (ticker||'').replace('USDT','');
  const slText = slPrice ? ` • SL: ${Number(slPrice).toLocaleString('en-US',{maximumFractionDigits:4})}` : '';

  const toast  = document.createElement('div');
  toast.className = `alert-toast ${isBuy?'buy':'sell'}`;
  toast.style.position = 'fixed';
  toast.innerHTML = `
    <div class="toast-icon">${icon}</div>
    <div class="toast-body">
      <div class="toast-title" style="color:${color}">${label} — ${base}/${tf||''}</div>
      <div class="toast-sub">@ ${price ? Number(price).toLocaleString('en-US',{maximumFractionDigits:4}) : '—'}${slText}</div>
    </div>
    <div class="toast-close" onclick="stopSignalAlert();this.closest('.alert-toast').remove()">×</div>
    <div class="toast-bar"></div>`;
  document.body.appendChild(toast);

  // Tự đóng sau 11s
  setTimeout(() => {
    if (toast.parentNode) {
      toast.style.animation = 'toastOut .35s ease forwards';
      setTimeout(() => toast.remove(), 350);
    }
  }, 11_000);
}

/* ── SSE Live Updates ── */
function connectSSE() {
  const es = new EventSource('/events');
  es.onopen = () => {
    document.getElementById('hdr-status').textContent = '● Live';
    document.getElementById('hdr-status').style.color = 'var(--buy)';
  };
  es.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      if (d.type === 'new_signal') {
        // Prepend new row
        const tbody = document.getElementById('signal-tbody');
        const row   = document.createElement('template');
        row.innerHTML = buildRow(d, true);
        tbody.prepend(row.content.firstChild);
        // Refresh confluence in bg
        refreshConfluence();
        // ── AUDIO + VISUAL ALERT ──
        playSignalAlert(d.action);
        showAlertToast(d.action, d.ticker, d.timeframe, d.price, d.sl_price);
      }
    } catch(e) {}
  };
  es.onerror = () => {
    document.getElementById('hdr-status').textContent = '⚠ Reconnecting…';
    document.getElementById('hdr-status').style.color = 'var(--gold)';
    setTimeout(connectSSE, 5000);
    es.close();
  };
}

/* ── Init & Polling ── */
async function init() {
  // Load initial prices via REST (fast start), then switch to WebSocket
  await Promise.all([refreshPricesREST(), refreshSignals(), refreshConfluence()]);
  connectSSE();
  connectPriceWS();            // real-time prices via Binance WS
  setInterval(refreshConfluence, 30_000);
  setInterval(refreshSignals,    15_000);
}
init();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


if __name__ == "__main__":
    uvicorn.run("webhook_server:app", host="0.0.0.0", port=PORT, log_level="info")
