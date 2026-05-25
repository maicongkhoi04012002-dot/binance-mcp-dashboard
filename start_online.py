"""
start_online.py
───────────────
Tự động khởi động toàn bộ hệ thống và đưa web lên internet:

  1. Kill các process cũ đang dùng port 8765
  2. Chạy webhook_server.py (dashboard)
  3. Chạy binance_feed.py (live signal engine)
  4. Chạy ngrok → lấy URL public → in ra terminal

Dùng:
    python start_online.py
"""

import subprocess
import sys
import time
import os
import json
import signal
import requests
from pathlib import Path

BASE_DIR = Path(__file__).parent
PORT     = 8765

CYAN  = "\033[96m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
RED   = "\033[91m"
BOLD  = "\033[1m"
RESET = "\033[0m"
BLUE  = "\033[94m"

def banner():
    print(f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════════════╗
║          📡  Binance MCP Dashboard — Online Deploy        ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")

def kill_port(port: int):
    """Kill bất kỳ process nào đang dùng port này."""
    try:
        r = subprocess.run(
            ["powershell", "-Command",
             f"(Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue).OwningProcess | Sort-Object -Unique"],
            capture_output=True, text=True
        )
        pids = [p.strip() for p in r.stdout.splitlines() if p.strip().isdigit()]
        for pid in pids:
            subprocess.run(["taskkill", "/PID", pid, "/F"], capture_output=True)
            print(f"  {YELLOW}⚡ Killed old process PID {pid} on port {port}{RESET}")
    except Exception:
        pass

def start_process(args: list, name: str, cwd: Path) -> subprocess.Popen:
    """Start một subprocess và return handle."""
    print(f"  {GREEN}▶ Starting {name}...{RESET}")
    proc = subprocess.Popen(
        args,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return proc

def wait_for_server(url: str, timeout: int = 20) -> bool:
    """Đợi server sẵn sàng."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False

def get_ngrok_url(timeout: int = 20) -> str | None:
    """Lấy URL public từ ngrok API."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get("http://localhost:4040/api/tunnels", timeout=2)
            data = r.json()
            tunnels = data.get("tunnels", [])
            for t in tunnels:
                url = t.get("public_url", "")
                if url.startswith("https://"):
                    return url
        except Exception:
            pass
        time.sleep(1)
    return None


def main():
    banner()
    procs = []

    # ── Bước 1: Kill port cũ ─────────────────────────────────────────────
    print(f"{BOLD}[1/4] Dọn dẹp port {PORT}...{RESET}")
    kill_port(PORT)
    time.sleep(1)

    # ── Bước 2: Chạy webhook_server ──────────────────────────────────────
    print(f"\n{BOLD}[2/4] Khởi động Dashboard Server...{RESET}")
    server_proc = start_process(
        [sys.executable, "webhook_server.py"],
        "webhook_server.py",
        BASE_DIR,
    )
    procs.append(("Dashboard", server_proc))

    print(f"  {YELLOW}⏳ Đợi server sẵn sàng...{RESET}", end="", flush=True)
    if wait_for_server(f"http://localhost:{PORT}/health"):
        print(f" {GREEN}✅ OK{RESET}")
    else:
        print(f" {RED}⚠ Server chưa sẵn sàng, tiếp tục...{RESET}")

    # ── Bước 3: Chạy binance_feed ────────────────────────────────────────
    print(f"\n{BOLD}[3/4] Khởi động Binance Feed (Signal Engine)...{RESET}")
    feed_proc = start_process(
        [sys.executable, "binance_feed.py"],
        "binance_feed.py",
        BASE_DIR,
    )
    procs.append(("BinanceFeed", feed_proc))
    time.sleep(2)

    # ── Bước 4: Chạy ngrok ───────────────────────────────────────────────
    print(f"\n{BOLD}[4/4] Khởi động ngrok tunnel...{RESET}")

    # Kill ngrok cũ nếu có
    subprocess.run(["taskkill", "/IM", "ngrok.exe", "/F"], capture_output=True)
    time.sleep(1)

    ngrok_exe = BASE_DIR / "ngrok.exe"
    if not ngrok_exe.exists():
        print(f"  {RED}❌ Không tìm thấy ngrok.exe trong {BASE_DIR}{RESET}")
        print(f"  {YELLOW}💡 Tải tại: https://ngrok.com/download{RESET}")
    else:
        ngrok_proc = subprocess.Popen(
            [str(ngrok_exe), "http", str(PORT)],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(("Ngrok", ngrok_proc))
        print(f"  {YELLOW}⏳ Đợi ngrok kết nối...{RESET}", end="", flush=True)
        public_url = get_ngrok_url(timeout=25)

        if public_url:
            print(f" {GREEN}✅ OK{RESET}")
            print(f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════════════╗
║                    🌐  LINK ONLINE CỦA BẠN               ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  {BLUE}{public_url:<56}{CYAN}║
║                                                          ║
║  ✅ Chia sẻ link này — truy cập từ bất kỳ đâu!          ║
║  📱 Mở trên điện thoại, máy tính khác đều được          ║
║  ⚠️  Link thay đổi mỗi lần restart (tài khoản free)     ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")
        else:
            print(f" {RED}❌ Không lấy được URL{RESET}")
            print(f"  {YELLOW}Kiểm tra ngrok tại: http://localhost:4040{RESET}")

    print(f"{YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"  {BOLD}📊 Local URL:{RESET}  http://localhost:{PORT}")
    print(f"  {BOLD}🛑 Dừng:{RESET}       Ctrl+C")
    print(f"{YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}\n")

    # ── Giữ chạy + in log ────────────────────────────────────────────────
    def shutdown(sig, frame):
        print(f"\n{YELLOW}🛑 Đang dừng tất cả...{RESET}")
        for name, p in procs:
            try:
                p.terminate()
                print(f"  Stopped {name}")
            except Exception:
                pass
        subprocess.run(["taskkill", "/IM", "ngrok.exe", "/F"], capture_output=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"{GREEN}✅ Tất cả đang chạy. Log dashboard:{RESET}\n")
    try:
        while True:
            # In log từ server
            line = server_proc.stdout.readline()
            if line:
                print(f"  {line}", end="")
            elif server_proc.poll() is not None:
                print(f"\n{RED}❌ Dashboard server đã dừng!{RESET}")
                break
            else:
                time.sleep(0.1)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
