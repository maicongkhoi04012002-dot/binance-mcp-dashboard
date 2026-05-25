# 📡 Binance MCP Signal Dashboard

Kết nối Binance real-time → AI Editor (Antigravity, Cursor, Claude Desktop) qua **MCP protocol**.

---

## 🏗️ Architecture

```
Binance WebSocket (15 streams)
      │  RSI(14) + EMA(9) + WMA(45) + MACD(12,26,9)
      ▼
binance_feed.py  ← BTC/ETH/BNB/SOL/XRP × 1D/4H/1H
      │  (SQLite)
      ▼
  signals.db
      │
      ├──► webhook_server.py  → Dashboard http://localhost:8765
      │        Live prices · MTF Confluence · Signal table · SSE push
      │
      ├──► mcp_server.py  ← stdio MCP (8 tools)
      │        AI Editor calls tools on-demand
      │
      └──► binance_trader.py  ← Execute orders (dry-run / testnet / live)
```

---

## 🚀 Setup nhanh

### 1. Install dependencies
```bash
pip install mcp fastapi uvicorn aiosqlite python-dotenv requests websockets
```

### 2. Config
```bash
copy .env.example .env
# Điền BINANCE_API_KEY / API_SECRET (tuỳ chọn)
# Mặc định: DRY_RUN=true, TESTNET=true → an toàn 100%
```

### 3. Chạy Binance Feed (background)
```bash
python binance_feed.py
# → 15 streams: BTC/ETH/BNB/SOL/XRP × 1D/4H/1H
# → Tự động lưu signal vào signals.db
```

### 4. Chạy Dashboard
```bash
python webhook_server.py
# → http://localhost:8765
# → Live prices, MTF confluence, signal table, SSE push
```

### 5. Cấu hình MCP cho Editor

**Antigravity / Cursor** — thêm vào MCP settings:
```json
{
  "mcpServers": {
    "tradingview": {
      "command": "python",
      "args": [
        "C:\\Users\\maico\\.gemini\\antigravity\\scratch\\tradingview-mcp\\mcp_server.py"
      ]
    }
  }
}
```

**Claude Desktop** — `%APPDATA%\Claude\claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "tradingview": {
      "command": "python",
      "args": ["C:\\path\\to\\mcp_server.py"]
    }
  }
}
```

---

## 🛠️ MCP Tools (8 tools)

### TradingView / DB Tools
| Tool | Mô tả |
|------|-------|
| `tv_get_latest_signals` | N signal mới nhất, filter theo ticker |
| `tv_get_active_signals` | Vị thế đang active (BUY/SELL chưa đóng) |
| `tv_get_signal_stats` | Thống kê BUY/SELL/total |
| `tv_get_signals_by_timeframe` | Filter theo ticker + timeframe |
| `tv_analyze_mtf_confluence` | Phân tích 1D/4H/1H, cho điểm confluence |

### Binance Live Data Tools
| Tool | Mô tả |
|------|-------|
| `binance_get_price` | Giá hiện tại + 24h stats |
| `binance_get_indicators` | Tính RSI/EMA/WMA live (không cần feed chạy) |
| `binance_get_orderbook` | Top bids/asks + bid/ask pressure ratio |

### Trading Execution Tools
| Tool | Mô tả |
|------|-------|
| `binance_execute_order` | Đặt lệnh BUY/SELL (dry-run mặc định) |
| `binance_positions` | Xem vị thế mở + live P&L |

### Ví dụ dùng trong AI chat:
```
"Phân tích MTF confluence cho BTCUSDT"
→ tv_analyze_mtf_confluence → confluence score 1D/4H/1H

"Giá ETH hiện tại và orderbook?"
→ binance_get_price + binance_get_orderbook

"Tính indicator SOLUSDT 4H"
→ binance_get_indicators(symbol=SOLUSDT, interval=4h)

"Mua thử $20 BTC (dry run)"
→ binance_execute_order(action=BUY, symbol=BTCUSDT, usdt_amount=20)
```

---

## ⚠️ Trading Safety

| Bước | Mô tả |
|------|-------|
| **DRY_RUN=true** | Chỉ simulate, KHÔNG đặt lệnh thật (mặc định) |
| **BINANCE_TESTNET=true** | Dùng Testnet Binance (tiền giả) |
| **BINANCE_TESTNET=false** + **DRY_RUN=false** | ⚠️ LIVE — cẩn thận! |

**Lấy API key Testnet**: https://testnet.binance.vision

**Risk Management** (cấu hình trong `.env`):
```
MAX_USDT_PER_TRADE=20    # Tối đa $20 mỗi lệnh
MAX_OPEN_TRADES=3        # Tối đa 3 vị thế
STOP_LOSS_PCT=2.0        # Stop loss 2%
TAKE_PROFIT_PCT=4.0      # Take profit 4%
```

---

## 💡 Tips tiết kiệm token

| Model | Dùng khi nào |
|-------|-------------|
| **DeepSeek-V3** | Phân tích data, viết code, backtest logic |
| **Qwen-72B** | Xử lý dữ liệu lớn, tóm tắt nhiều signals |
| **Claude Sonnet** | Phân tích chiến lược phức tạp, reasoning |

---

## 📁 File structure
```
tradingview-mcp/
├── binance_feed.py       # WebSocket feed: BTC/ETH/BNB/SOL/XRP × 3 TF
├── binance_trader.py     # Binance order execution (dry-run / testnet / live)
├── webhook_server.py     # FastAPI: dashboard + webhook receiver
├── mcp_server.py         # MCP stdio server (8 tools)
├── database.py           # SQLite layer
├── mcp.json              # Config cho Cursor/Antigravity
├── .env.example          # Config template
├── signals.db            # Auto-created
└── README.md
```
