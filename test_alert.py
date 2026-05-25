# -*- coding: utf-8 -*-
import sys, io, time, requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

print("=== TEST ALERT ===")

# 1. System beep
try:
    import winsound
    print("[1] BUY beep 880Hz x 3s...")
    end = time.time() + 3
    while time.time() < end:
        winsound.Beep(880, 400)
    print("    OK - nghe duoc khong?")
    time.sleep(0.5)
    print("[1] SELL beep 440Hz x 3s...")
    end = time.time() + 3
    while time.time() < end:
        winsound.Beep(440, 400)
    print("    OK")
except Exception as e:
    print(f"    WARN: {e}")

# 2. Webhook + browser toast
print("[2] Check server http://localhost:8765 ...")
try:
    r = requests.get("http://localhost:8765/health", timeout=3)
    print(f"    Server OK: {r.json()}")
except Exception as e:
    print(f"    ERROR server: {e}")
    sys.exit(1)

buy_payload = {
    "action": "BUY", "ticker": "BTCUSDT", "timeframe": "60",
    "price": 97500.5, "rsi": 52.3, "ema9": 97200.0, "wma45": 96800.0,
    "sl_price": 95650.0, "sl_method": "swing_low", "sl_pct": 1.9,
    "source": "test_alert"
}
r = requests.post("http://localhost:8765/webhook", json=buy_payload, timeout=5)
sig_id = r.json().get("id")
print(f"    BUY signal sent -> id={sig_id}")
print("    -> Check browser: toast xanh + chuong tren trang web!")

time.sleep(3)

sell_payload = {
    "action": "SELL", "ticker": "ETHUSDT", "timeframe": "240",
    "price": 1820.3, "rsi": 47.1, "ema9": 1835.0, "wma45": 1855.0,
    "sl_price": 1858.2, "sl_method": "atr_14", "sl_pct": 2.1,
    "source": "test_alert"
}
r = requests.post("http://localhost:8765/webhook", json=sell_payload, timeout=5)
sig_id = r.json().get("id")
print(f"    SELL signal sent -> id={sig_id}")
print("    -> Check browser: toast do + chuong tren trang web!")

print("")
print("=== DONE ===")
print("Mo http://localhost:8765 de xem toast notification!")
