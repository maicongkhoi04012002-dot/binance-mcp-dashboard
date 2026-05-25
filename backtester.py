"""
backtester.py
─────────────
Backtest Engine — tải toàn bộ dữ liệu lịch sử từ Binance, chạy lại strategy
RSI+EMA+WMA+MACD, tính win rate, PnL, drawdown, và tối ưu tham số bằng ML.

Chức năng:
  - Fetch tới 1000 candles lịch sử (có thể loop để lấy thêm)
  - Backtest mỗi cặp × mỗi timeframe với sliding window
  - Tính: Win Rate, Avg PnL, Max Drawdown, Sharpe, Sortino
  - Grid Search tối ưu tham số RSI/EMA/WMA/MACD/VOL_FACTOR
  - Trả kết quả JSON cho dashboard
"""

import requests
import math
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

BINANCE_REST = "https://api.binance.com/api/v3/klines"

# ─── DEFAULT STRATEGY PARAMS ─────────────────────────────────────────────────

DEFAULT_PARAMS = {
    "rsi_len":      14,
    "ema_len":      9,
    "wma_len":      45,
    "macd_fast":    12,
    "macd_slow":    26,
    "macd_sig":     9,
    "vol_factor":   1.0,    # volume filter multiplier
    "tp_pct":       3.0,    # take profit %
    "sl_pct":       1.5,    # stop loss %
    "rsi_bull":     50,     # RSI threshold for bullish
    "rsi_bear":     50,     # RSI threshold for bearish
}

# ─── INDICATOR HELPERS ────────────────────────────────────────────────────────

def _calc_rsi(closes: list, period: int) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag/al), 4)


def _calc_ema(closes: list, period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for p in closes[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _calc_wma(closes: list, period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    weights = list(range(1, period + 1))
    subset  = closes[-period:]
    return sum(p * w for p, w in zip(subset, weights)) / sum(weights)


def _calc_macd(closes: list, fast: int, slow: int, sig: int):
    if len(closes) < slow + sig:
        return None, None, None

    def ema(data, period):
        k = 2 / (period + 1)
        e = sum(data[:period]) / period
        for p in data[period:]:
            e = p * k + e * (1 - k)
        return e

    macd_hist_series = []
    for i in range(slow, len(closes) + 1):
        w = closes[:i]
        macd_hist_series.append(ema(w, fast) - ema(w, slow))

    if len(macd_hist_series) < sig:
        return None, None, None

    sig_k   = 2 / (sig + 1)
    sig_ema = sum(macd_hist_series[:sig]) / sig
    for v in macd_hist_series[sig:]:
        sig_ema = v * sig_k + sig_ema * (1 - sig_k)

    macd_line = macd_hist_series[-1]
    hist      = macd_line - sig_ema
    return macd_line, sig_ema, hist


def _detect_signal(closes, highs, lows, volumes, params: dict) -> str:
    rsi   = _calc_rsi(closes, params["rsi_len"])
    ema9  = _calc_ema(closes, params["ema_len"])
    wma45 = _calc_wma(closes, params["wma_len"])
    _, _, hist = _calc_macd(closes, params["macd_fast"], params["macd_slow"], params["macd_sig"])

    if None in (rsi, ema9, wma45, hist):
        return "NEUTRAL"

    vol_avg = sum(volumes) / len(volumes) if volumes else 0
    vol_cur = volumes[-1] if volumes else 0
    vol_ok  = (vol_avg == 0) or (vol_cur >= vol_avg * params["vol_factor"])

    bullish = ema9 > wma45 and rsi > params["rsi_bull"] and hist > 0 and vol_ok
    bearish = ema9 < wma45 and rsi < params["rsi_bear"] and hist < 0 and vol_ok

    if bullish:
        return "BUY"
    if bearish:
        return "SELL"
    return "NEUTRAL"


# ─── FETCH HISTORICAL DATA ────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str, interval: str, limit: int = 500) -> list[dict]:
    """
    Fetch up to `limit` candles from Binance REST API.
    Returns list of dicts: {time, open, high, low, close, volume}
    Max 1000 per call, we paginate to get more.
    """
    all_klines = []
    max_per_call = 1000
    end_time = None

    remaining = limit
    while remaining > 0:
        fetch_n = min(remaining, max_per_call)
        params  = {
            "symbol":   symbol,
            "interval": interval,
            "limit":    fetch_n,
        }
        if end_time:
            params["endTime"] = end_time

        try:
            resp = requests.get(BINANCE_REST, params=params, timeout=15)
            resp.raise_for_status()
            klines = resp.json()
        except Exception as e:
            log.error(f"Fetch error {symbol} {interval}: {e}")
            break

        if not klines:
            break

        all_klines = klines + all_klines
        end_time  = klines[0][0] - 1   # go back before first candle
        remaining -= len(klines)

        if len(klines) < fetch_n:
            break   # no more data

    result = []
    for k in all_klines[:-1]:  # exclude last (possibly open) candle
        result.append({
            "time":   datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).isoformat(),
            "open":   float(k[1]),
            "high":   float(k[2]),
            "low":    float(k[3]),
            "close":  float(k[4]),
            "volume": float(k[5]),
        })
    return result


# ─── BACKTEST CORE ────────────────────────────────────────────────────────────

def run_backtest(
    candles: list[dict],
    params: dict,
    symbol: str = "UNKNOWN",
    timeframe: str = "?",
) -> dict:
    """
    Simulate the strategy on `candles` using a fixed TP/SL.
    Returns a rich performance report.
    """
    if not candles:
        return {"error": "No candles", "symbol": symbol, "timeframe": timeframe}

    warmup = max(
        params["rsi_len"],
        params["ema_len"],
        params["wma_len"],
        params["macd_slow"] + params["macd_sig"],
    ) + 5

    closes  = [c["close"]  for c in candles]
    highs   = [c["high"]   for c in candles]
    lows    = [c["low"]    for c in candles]
    volumes = [c["volume"] for c in candles]
    times   = [c["time"]   for c in candles]

    trades = []
    prev_signal = "NEUTRAL"
    in_trade    = None          # {side, entry, sl, tp, entry_idx, entry_time}

    for i in range(warmup, len(closes)):
        cl_w  = closes[:i+1]
        hi_w  = highs[:i+1]
        lo_w  = lows[:i+1]
        vo_w  = volumes[:i+1]

        # ── Check if existing trade hits TP/SL ─────────────────────────────
        if in_trade:
            c_hi = highs[i]
            c_lo = lows[i]
            c_cl = closes[i]

            if in_trade["side"] == "BUY":
                if c_lo <= in_trade["sl"]:
                    pnl = (in_trade["sl"] - in_trade["entry"]) / in_trade["entry"] * 100
                    trades.append(_make_trade(in_trade, "LOSS", pnl, times[i], closes[i]))
                    in_trade = None
                elif c_hi >= in_trade["tp"]:
                    pnl = (in_trade["tp"] - in_trade["entry"]) / in_trade["entry"] * 100
                    trades.append(_make_trade(in_trade, "WIN", pnl, times[i], closes[i]))
                    in_trade = None
            else:  # SELL
                if c_hi >= in_trade["sl"]:
                    pnl = (in_trade["entry"] - in_trade["sl"]) / in_trade["entry"] * 100
                    trades.append(_make_trade(in_trade, "LOSS", -abs(pnl), times[i], closes[i]))
                    in_trade = None
                elif c_lo <= in_trade["tp"]:
                    pnl = (in_trade["entry"] - in_trade["tp"]) / in_trade["entry"] * 100
                    trades.append(_make_trade(in_trade, "WIN", abs(pnl), times[i], closes[i]))
                    in_trade = None

        # ── Detect new signal ─────────────────────────────────────────────
        signal = _detect_signal(cl_w, hi_w, lo_w, vo_w, params)

        if signal != "NEUTRAL" and signal != prev_signal and in_trade is None:
            entry = closes[i]
            sl_d  = params["sl_pct"] / 100
            tp_d  = params["tp_pct"] / 100

            if signal == "BUY":
                sl_price = entry * (1 - sl_d)
                tp_price = entry * (1 + tp_d)
            else:
                sl_price = entry * (1 + sl_d)
                tp_price = entry * (1 - tp_d)

            in_trade = {
                "side":       signal,
                "entry":      entry,
                "sl":         sl_price,
                "tp":         tp_price,
                "entry_idx":  i,
                "entry_time": times[i],
            }

        prev_signal = signal

    # ── Close any open trade at last candle ──────────────────────────────
    if in_trade and closes:
        last_close = closes[-1]
        if in_trade["side"] == "BUY":
            pnl = (last_close - in_trade["entry"]) / in_trade["entry"] * 100
        else:
            pnl = (in_trade["entry"] - last_close) / in_trade["entry"] * 100
        result = "WIN" if pnl > 0 else "LOSS"
        trades.append(_make_trade(in_trade, result, pnl, times[-1], last_close, open_trade=True))

    return _compute_stats(trades, symbol, timeframe, params, len(candles))


def _make_trade(t: dict, result: str, pnl: float, exit_time: str, exit_price: float,
                open_trade: bool = False) -> dict:
    return {
        "side":        t["side"],
        "entry_time":  t["entry_time"],
        "exit_time":   exit_time,
        "entry_price": round(t["entry"], 6),
        "exit_price":  round(exit_price, 6),
        "sl":          round(t["sl"], 6),
        "tp":          round(t["tp"], 6),
        "result":      result,
        "pnl_pct":     round(pnl, 4),
        "open":        open_trade,
    }


def _compute_stats(trades: list, symbol: str, timeframe: str,
                   params: dict, n_candles: int) -> dict:
    if not trades:
        return {
            "symbol": symbol, "timeframe": timeframe,
            "n_candles": n_candles, "n_trades": 0,
            "win_rate": 0, "avg_pnl": 0, "total_pnl": 0,
            "max_drawdown": 0, "sharpe": 0, "profit_factor": 0,
            "params": params, "trades": [],
        }

    wins   = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    pnls   = [t["pnl_pct"] for t in trades]

    win_rate = round(len(wins) / len(trades) * 100, 2)
    avg_pnl  = round(sum(pnls) / len(pnls), 4)
    total_pnl= round(sum(pnls), 4)

    # Max drawdown (on cumulative equity)
    equity = [100.0]
    for pnl in pnls:
        equity.append(equity[-1] * (1 + pnl/100))
    peak = equity[0]
    max_dd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe (daily returns simplified)
    if len(pnls) > 1:
        mean_r = sum(pnls) / len(pnls)
        var_r  = sum((r - mean_r)**2 for r in pnls) / (len(pnls) - 1)
        std_r  = math.sqrt(var_r) if var_r > 0 else 1e-9
        sharpe = round(mean_r / std_r * math.sqrt(252), 4)
    else:
        sharpe = 0

    # Profit factor
    gross_win  = sum(t["pnl_pct"] for t in wins)  if wins  else 0
    gross_loss = abs(sum(t["pnl_pct"] for t in losses)) if losses else 1e-9
    profit_factor = round(gross_win / gross_loss, 4) if gross_loss else 999.0

    # Sortino
    neg_returns = [r for r in pnls if r < 0]
    if neg_returns and len(neg_returns) > 1:
        mean_neg = sum(neg_returns) / len(neg_returns)
        var_neg  = sum((r - mean_neg)**2 for r in neg_returns) / (len(neg_returns) - 1)
        std_neg  = math.sqrt(var_neg) if var_neg > 0 else 1e-9
        sortino  = round(avg_pnl / std_neg * math.sqrt(252), 4)
    else:
        sortino  = sharpe

    # Monthly win rate breakdown
    monthly = {}
    for t in trades:
        m = t["entry_time"][:7]   # "YYYY-MM"
        if m not in monthly:
            monthly[m] = {"wins": 0, "losses": 0, "pnl": 0}
        if t["result"] == "WIN":
            monthly[m]["wins"] += 1
        else:
            monthly[m]["losses"] += 1
        monthly[m]["pnl"] += t["pnl_pct"]

    return {
        "symbol":        symbol,
        "timeframe":     timeframe,
        "n_candles":     n_candles,
        "n_trades":      len(trades),
        "n_wins":        len(wins),
        "n_losses":      len(losses),
        "win_rate":      win_rate,
        "avg_pnl":       avg_pnl,
        "total_pnl":     total_pnl,
        "max_drawdown":  round(max_dd, 4),
        "sharpe":        sharpe,
        "sortino":       sortino,
        "profit_factor": profit_factor,
        "final_equity":  round(equity[-1], 4),
        "equity_curve":  [round(v, 4) for v in equity],
        "params":        params,
        "monthly":       monthly,
        "trades":        trades[-100:],   # last 100 trades for UI
    }


# ─── GRID SEARCH OPTIMIZER ───────────────────────────────────────────────────

OPTIMIZER_GRID = {
    "rsi_len":    [10, 14, 20],
    "ema_len":    [7, 9, 12],
    "wma_len":    [30, 45, 55],
    "rsi_bull":   [45, 50, 55],
    "rsi_bear":   [45, 50, 55],
    "vol_factor": [1.0, 1.2, 1.5],
    "tp_pct":     [2.0, 3.0, 5.0],
    "sl_pct":     [1.0, 1.5, 2.0],
}


def optimize_params(candles: list[dict], symbol: str, timeframe: str,
                    metric: str = "sharpe") -> dict:
    """
    Grid search over OPTIMIZER_GRID, return top 5 param sets ranked by `metric`.
    `metric` can be: "win_rate", "sharpe", "profit_factor", "total_pnl"
    """
    if not candles or len(candles) < 100:
        return {"error": "Need at least 100 candles for optimization"}

    import itertools
    keys   = list(OPTIMIZER_GRID.keys())
    values = list(OPTIMIZER_GRID.values())

    best_results = []

    for combo in itertools.product(*values):
        p = dict(zip(keys, combo))
        p["macd_fast"] = DEFAULT_PARAMS["macd_fast"]
        p["macd_slow"] = DEFAULT_PARAMS["macd_slow"]
        p["macd_sig"]  = DEFAULT_PARAMS["macd_sig"]

        # Skip invalid combos
        if p["sl_pct"] >= p["tp_pct"]:
            continue
        if p["ema_len"] >= p["wma_len"]:
            continue

        r = run_backtest(candles, p, symbol, timeframe)
        if r.get("n_trades", 0) < 5:
            continue

        score = r.get(metric, 0)
        best_results.append({
            "score":         round(score, 4),
            "win_rate":      r["win_rate"],
            "sharpe":        r["sharpe"],
            "profit_factor": r["profit_factor"],
            "total_pnl":     r["total_pnl"],
            "n_trades":      r["n_trades"],
            "max_drawdown":  r["max_drawdown"],
            "params":        p,
        })

    best_results.sort(key=lambda x: x["score"], reverse=True)
    top5 = best_results[:5]

    return {
        "symbol":    symbol,
        "timeframe": timeframe,
        "metric":    metric,
        "total_combos_tested": len(best_results),
        "top_results": top5,
        "best_params": top5[0]["params"] if top5 else DEFAULT_PARAMS,
    }


# ─── MULTI-PAIR FULL BACKTEST ─────────────────────────────────────────────────

BACKTEST_CONFIGS = [
    ("BTCUSDT",  "1d",  "1D"),
    ("BTCUSDT",  "4h",  "4H"),
    ("BTCUSDT",  "1h",  "1H"),
    ("ETHUSDT",  "1d",  "1D"),
    ("ETHUSDT",  "4h",  "4H"),
    ("ETHUSDT",  "1h",  "1H"),
    ("BNBUSDT",  "1d",  "1D"),
    ("BNBUSDT",  "4h",  "4H"),
    ("SOLUSDT",  "1d",  "1D"),
    ("SOLUSDT",  "4h",  "4H"),
    ("XRPUSDT",  "1d",  "1D"),
    ("XRPUSDT",  "4h",  "4H"),
]


def run_full_backtest(n_candles: int = 500, params: dict = None) -> dict:
    """
    Run backtest across all configured pairs/timeframes.
    Returns aggregate summary + per-pair results.
    """
    params = params or DEFAULT_PARAMS
    results = []

    for symbol, interval, tf_label in BACKTEST_CONFIGS:
        log.info(f"Backtesting {symbol} [{tf_label}] — fetching {n_candles} candles...")
        try:
            candles = fetch_ohlcv(symbol, interval, limit=n_candles)
            if not candles:
                continue
            r = run_backtest(candles, params, symbol, tf_label)
            results.append(r)
            log.info(
                f"  {symbol} [{tf_label}]: {r['n_trades']} trades | "
                f"WR={r['win_rate']}% | PnL={r['total_pnl']}% | "
                f"Sharpe={r['sharpe']}"
            )
        except Exception as e:
            log.error(f"Error backtesting {symbol} [{tf_label}]: {e}")

    if not results:
        return {"error": "No backtest results"}

    # Aggregate stats
    all_trades = sum(r["n_trades"]  for r in results)
    all_wins   = sum(r["n_wins"]    for r in results if "n_wins"   in r)
    all_losses = sum(r["n_losses"]  for r in results if "n_losses" in r)
    avg_wr     = round(all_wins / all_trades * 100, 2) if all_trades else 0
    avg_sharpe = round(sum(r["sharpe"] for r in results) / len(results), 4)
    avg_pf     = round(sum(r["profit_factor"] for r in results) / len(results), 4)
    avg_dd     = round(sum(r["max_drawdown"]  for r in results) / len(results), 4)

    return {
        "timestamp":   datetime.now(tz=timezone.utc).isoformat(),
        "n_candles":   n_candles,
        "n_pairs":     len(results),
        "params":      params,
        "aggregate": {
            "total_trades":  all_trades,
            "total_wins":    all_wins,
            "total_losses":  all_losses,
            "win_rate":      avg_wr,
            "avg_sharpe":    avg_sharpe,
            "avg_profit_factor": avg_pf,
            "avg_max_drawdown":  avg_dd,
        },
        "results": results,
    }


# ─── STANDALONE ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BACKTEST] %(message)s",
        datefmt="%H:%M:%S",
    )
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300

    print(f"\n{'='*60}")
    print(f"  FULL BACKTEST — {n} candles per pair")
    print(f"{'='*60}")

    report = run_full_backtest(n_candles=n)
    agg = report.get("aggregate", {})

    print(f"\n📊 AGGREGATE RESULTS ({report['n_pairs']} pairs):")
    print(f"  Total Trades  : {agg.get('total_trades', 0)}")
    print(f"  Win Rate      : {agg.get('win_rate', 0):.2f}%")
    print(f"  Avg Sharpe    : {agg.get('avg_sharpe', 0):.4f}")
    print(f"  Avg PF        : {agg.get('avg_profit_factor', 0):.4f}")
    print(f"  Avg Drawdown  : {agg.get('avg_max_drawdown', 0):.2f}%")

    print(f"\n{'─'*60}")
    for r in report.get("results", []):
        wr_col = "✅" if r["win_rate"] >= 55 else ("⚠️ " if r["win_rate"] >= 45 else "❌")
        print(
            f"  {wr_col} {r['symbol']:10} [{r['timeframe']:3}] "
            f"Trades:{r['n_trades']:4}  "
            f"WR:{r['win_rate']:5.1f}%  "
            f"PnL:{r['total_pnl']:8.2f}%  "
            f"Sharpe:{r['sharpe']:6.3f}  "
            f"MaxDD:{r['max_drawdown']:5.2f}%"
        )

    print(f"\n{'='*60}")
    print("✨ Run with --optimize to find best params")
