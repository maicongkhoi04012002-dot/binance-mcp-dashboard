"""
email_notifier.py
─────────────────
Gửi email HTML đẹp qua Gmail SMTP khi có signal hoặc SL bị hit.

Setup:
  1. Bật 2-Step Verification cho Gmail:
     https://myaccount.google.com/security
  2. Tạo App Password (16 ký tự):
     https://myaccount.google.com/apppasswords
     → Chọn "Mail" + tên thiết bị → Copy password
  3. Thêm vào .env:
     EMAIL_SENDER=your_gmail@gmail.com
     EMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
     EMAIL_RECIPIENT=maicongkhoi04012002@gmail.com
"""

import asyncio
import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

EMAIL_SENDER    = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD  = os.getenv("EMAIL_APP_PASSWORD", "")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "maicongkhoi04012002@gmail.com")
SMTP_HOST       = "smtp.gmail.com"
SMTP_PORT       = 465


# ─── HTML TEMPLATES ──────────────────────────────────────────────────────────

def _signal_html(
    symbol: str,
    timeframe: str,
    action: str,
    price: float,
    rsi: float | None,
    ema9: float | None,
    wma45: float | None,
    macd_hist: float | None,
    stop_loss: float | None,
    take_profit: float | None,
    signal_id: int,
) -> tuple[str, str]:
    """Returns (subject, html_body)"""

    is_buy    = action.upper() in ("BUY", "LONG")
    color     = "#10b981" if is_buy else "#ef4444"
    bg_color  = "#0a2e1e" if is_buy else "#2e0a0a"
    icon      = "🟢" if is_buy else "🔴"
    arrow     = "▲" if is_buy else "▼"
    label     = "BUY SIGNAL" if is_buy else "SELL SIGNAL"

    sl_row  = f"""<tr>
      <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">Stop Loss</td>
      <td style="padding:10px 16px;font-weight:600;color:#ef4444;border-bottom:1px solid #1e293b">
        {f'${stop_loss:,.4f}' if stop_loss else '—'}
      </td>
    </tr>""" if stop_loss else ""

    tp_row  = f"""<tr>
      <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">Take Profit</td>
      <td style="padding:10px 16px;font-weight:600;color:#10b981;border-bottom:1px solid #1e293b">
        {f'${take_profit:,.4f}' if take_profit else '—'}
      </td>
    </tr>""" if take_profit else ""

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    subject = f"{icon} [{label}] {symbol} [{timeframe}] @ ${price:,.2f}"

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#1e293b;border-radius:16px;overflow:hidden;border:1px solid #334155;">

        <!-- Header -->
        <tr>
          <td style="background:{bg_color};padding:28px 32px;border-bottom:2px solid {color};">
            <div style="font-size:11px;font-weight:700;letter-spacing:3px;color:{color};margin-bottom:8px;">
              {label}
            </div>
            <div style="font-size:32px;font-weight:800;color:#f1f5f9;letter-spacing:-1px;">
              {symbol}
            </div>
            <div style="font-size:14px;color:#94a3b8;margin-top:4px;">
              Timeframe: <b style="color:#e2e8f0">{timeframe}</b>
              &nbsp;&bull;&nbsp; Signal #{signal_id}
              &nbsp;&bull;&nbsp; {now}
            </div>
          </td>
        </tr>

        <!-- Price Banner -->
        <tr>
          <td style="background:#0f172a;padding:20px 32px;text-align:center;border-bottom:1px solid #1e293b;">
            <div style="font-size:13px;color:#64748b;margin-bottom:4px;">Current Price</div>
            <div style="font-size:40px;font-weight:800;color:{color};font-family:monospace;">
              {arrow} ${price:,.4f}
            </div>
          </td>
        </tr>

        <!-- Indicators Table -->
        <tr>
          <td style="padding:0 32px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:20px;">
              <tr style="background:#0f172a;">
                <td style="padding:8px 16px;font-size:11px;font-weight:700;color:#64748b;letter-spacing:1px;border-radius:6px 0 0 0">INDICATOR</td>
                <td style="padding:8px 16px;font-size:11px;font-weight:700;color:#64748b;letter-spacing:1px;border-radius:0 6px 0 0">VALUE</td>
              </tr>
              <tr>
                <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">RSI (14)</td>
                <td style="padding:10px 16px;font-weight:600;color:{'#ef4444' if rsi and rsi > 70 else '#10b981' if rsi and rsi < 30 else '#e2e8f0'};font-family:monospace;border-bottom:1px solid #1e293b">
                  {rsi if rsi is not None else '—'}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">EMA (9)</td>
                <td style="padding:10px 16px;font-weight:600;color:#e2e8f0;font-family:monospace;border-bottom:1px solid #1e293b">
                  ${ema9:,.4f}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">WMA (45)</td>
                <td style="padding:10px 16px;font-weight:600;color:#e2e8f0;font-family:monospace;border-bottom:1px solid #1e293b">
                  ${wma45:,.4f}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">MACD Histogram</td>
                <td style="padding:10px 16px;font-weight:600;color:{'#10b981' if macd_hist and macd_hist > 0 else '#ef4444' if macd_hist and macd_hist < 0 else '#94a3b8'};font-family:monospace;border-bottom:1px solid #1e293b">
                  {macd_hist if macd_hist is not None else '—'}
                </td>
              </tr>
              {sl_row}
              {tp_row}
            </table>
          </td>
        </tr>

        <!-- CTA -->
        <tr>
          <td style="padding:0 32px 28px;text-align:center;">
            <div style="background:{color};color:#fff;padding:12px 28px;border-radius:8px;
                        font-size:14px;font-weight:700;letter-spacing:.5px;display:inline-block;">
              {icon} {label} — {symbol} @ ${price:,.4f}
            </div>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="background:#0a0f1e;padding:16px 32px;border-top:1px solid #1e293b;text-align:center;">
            <div style="font-size:11px;color:#475569;">
              Binance MCP Signal Dashboard · Auto-generated alert · Do not reply
            </div>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, html


def _stoploss_html(
    symbol: str,
    timeframe: str,
    entry_price: float,
    exit_price: float,
    stop_loss: float,
    qty: float,
    pnl_pct: float,
) -> tuple[str, str]:
    """Returns (subject, html_body) for SL-triggered alert."""

    pnl_color = "#10b981" if pnl_pct >= 0 else "#ef4444"
    now       = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    pnl_sign  = "+" if pnl_pct >= 0 else ""
    subject   = f"⛔ [STOP LOSS HIT] {symbol} — PnL {pnl_sign}{pnl_pct:.2f}%"

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#1e293b;border-radius:16px;overflow:hidden;border:1px solid #334155;">

        <!-- Header -->
        <tr>
          <td style="background:#2d0a0a;padding:28px 32px;border-bottom:2px solid #ef4444;">
            <div style="font-size:11px;font-weight:700;letter-spacing:3px;color:#ef4444;margin-bottom:8px;">
              ⛔ STOP LOSS TRIGGERED
            </div>
            <div style="font-size:32px;font-weight:800;color:#f1f5f9;">{symbol}</div>
            <div style="font-size:13px;color:#94a3b8;margin-top:4px;">{now}</div>
          </td>
        </tr>

        <!-- PnL Banner -->
        <tr>
          <td style="background:#0f172a;padding:20px 32px;text-align:center;border-bottom:1px solid #1e293b;">
            <div style="font-size:13px;color:#64748b;margin-bottom:4px;">Realized PnL</div>
            <div style="font-size:40px;font-weight:800;color:{pnl_color};font-family:monospace;">
              {pnl_sign}{pnl_pct:.2f}%
            </div>
          </td>
        </tr>

        <!-- Details -->
        <tr>
          <td style="padding:0 32px 28px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:20px;">
              <tr><td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">Entry Price</td>
                  <td style="padding:10px 16px;font-family:monospace;color:#e2e8f0;border-bottom:1px solid #1e293b">${entry_price:,.4f}</td></tr>
              <tr><td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">Stop Loss</td>
                  <td style="padding:10px 16px;font-family:monospace;color:#ef4444;font-weight:700;border-bottom:1px solid #1e293b">${stop_loss:,.4f}</td></tr>
              <tr><td style="padding:10px 16px;color:#94a3b8;border-bottom:1px solid #1e293b">Exit Price</td>
                  <td style="padding:10px 16px;font-family:monospace;color:#ef4444;border-bottom:1px solid #1e293b">${exit_price:,.4f}</td></tr>
              <tr><td style="padding:10px 16px;color:#94a3b8">Qty</td>
                  <td style="padding:10px 16px;font-family:monospace;color:#e2e8f0">{qty:.6f} {symbol.replace('USDT','')}</td></tr>
            </table>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="background:#0a0f1e;padding:16px 32px;border-top:1px solid #1e293b;text-align:center;">
            <div style="font-size:11px;color:#475569;">
              Binance MCP Signal Dashboard · Stop Loss Monitor · Auto-executed
            </div>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, html


# ─── SEND FUNCTION ────────────────────────────────────────────────────────────

def _send_email_sync(subject: str, html: str, to: str = None):
    """Synchronous email send via Gmail SMTP SSL."""
    recipient = to or EMAIL_RECIPIENT

    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        log.warning(
            "Email not configured — set EMAIL_SENDER and EMAIL_APP_PASSWORD in .env\n"
            f"  Would have sent: {subject}"
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Binance MCP Alert <{EMAIL_SENDER}>"
    msg["To"]      = recipient
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, recipient, msg.as_bytes())
        log.info(f"📧 Email sent → {recipient}  Subject: {subject}")
        return True
    except Exception as e:
        log.error(f"Email send failed: {e}")
        return False


async def send_signal_email(
    symbol: str,
    timeframe: str,
    action: str,
    price: float,
    rsi: float | None   = None,
    ema9: float | None  = None,
    wma45: float | None = None,
    macd_hist: float | None = None,
    stop_loss: float | None = None,
    take_profit: float | None = None,
    signal_id: int = 0,
    to: str = None,
):
    """Async wrapper — sends signal alert email without blocking event loop."""
    subject, html = _signal_html(
        symbol, timeframe, action, price,
        rsi, ema9, wma45, macd_hist,
        stop_loss, take_profit, signal_id,
    )
    await asyncio.to_thread(_send_email_sync, subject, html, to)


async def send_stoploss_email(
    symbol: str,
    timeframe: str,
    entry_price: float,
    exit_price: float,
    stop_loss: float,
    qty: float,
    pnl_pct: float,
    to: str = None,
):
    """Async wrapper — sends stop-loss triggered alert."""
    subject, html = _stoploss_html(
        symbol, timeframe, entry_price, exit_price, stop_loss, qty, pnl_pct,
    )
    await asyncio.to_thread(_send_email_sync, subject, html, to)


# ─── QUICK TEST ───────────────────────────────────────────────────────────────

async def _test():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print("Sending test signal email...")
    await send_signal_email(
        symbol="BTCUSDT", timeframe="1H", action="BUY",
        price=80_500.12, rsi=62.5, ema9=79_800.0, wma45=78_200.0,
        macd_hist=120.5, stop_loss=78_890.12, take_profit=83_720.12,
        signal_id=999,
    )
    print("Sending test SL email...")
    await send_stoploss_email(
        symbol="BTCUSDT", timeframe="1H",
        entry_price=80_500.0, exit_price=78_890.0,
        stop_loss=78_890.0, qty=0.00024, pnl_pct=-2.0,
    )


if __name__ == "__main__":
    asyncio.run(_test())
