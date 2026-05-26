# Vol Surge v5 — 1min Bot (BTC/USD)

## Overview
WebSocket-native trading bot for **BTCUSD Perpetual** on **Delta Exchange India**.
Detects Vol Surge signals from live 1-minute candles — no TradingView webhook dependency.

**Live URL:** https://millionare-shivji-1min-bot.fly.dev/dashboard

---

## Architecture
```
Delta WebSocket (candlestick_1m)
        ↓
  CandleFeed (candle_feed.py)       ← buffers 300 bars, Heikin-Ashi conversion
        ↓
  SignalEngine (signal_engine.py)   ← Vol Surge v5 Pine-parity logic
        ↓
  volsurge_v5_live.py               ← entry/exit execution + dashboard
        ↓
  Delta Exchange REST API           ← market entry + TP limit order
```

---

## Strategy

### Signal: Vol Surge v5
- **Candle type:** Heikin-Ashi (matches TradingView 78% WR mode)
- **Lookback:** 5 bars (`VS_LOOKBACK=5`)
- **Burst detection:** candle body ≥ `chopAvgTR × 2.0` (`VS_BURST_MULT=2.0`)
- **Cooldown:** 3 bars after signal (`VS_COOLDOWN=3`)
- **Min body filter:** 50pts (`MIN_BODY_PTS=50`, `USE_MIN_BODY=True`)
- **Session filter:** OFF (trades 24/7)
- **EMA filter:** OFF

### SL/TP Model
| Parameter | Value | Notes |
|---|---|---|
| SL | **150 pts fixed** | Software SL (Delta India rejects exchange stop orders) |
| TP | **200 pts fixed** | GTC limit order placed on Delta immediately after entry |
| Mode | Fixed (not ATR-based) | `FIXED_SL_PTS=150`, `FIXED_TP_PTS=200` |

### Entry
- **Type:** Market order (IOC) at bar close
- **Stale guard:** Skip if bar closed >30s ago (`MAX_SIGNAL_AGE_S=30`)
- **Limit timeout:** 45s (`ENTRY_LIMIT_TIMEOUT_S=45`)
- **Idempotent orders:** `client_order_id=uuid4.hex` prevents duplicate fills

### Exit
- **TP:** GTC limit order on Delta — auto-fills when price hits level
- **SL:** Software monitor polls price every 1s — sends market close order when breached
- **Manual:** `/api/close` endpoint or "Close Trade" button on dashboard

---

## Files

| File | Purpose |
|---|---|
| `volsurge_v5_live.py` | Main bot — signal handling, order execution, FastAPI dashboard |
| `candle_feed.py` | Delta WebSocket 1m candle feed + REST backfill (300 bars) |
| `signal_engine.py` | Vol Surge signal logic (timeframe-agnostic, Pine-parity) |
| `Dockerfile` | Docker image — python:3.11-slim + uvicorn |
| `requirements_v5.txt` | Python deps: websockets, fastapi, uvicorn, requests |
| `fly.toml` | Fly.io config — app: `millionare-shivji-1min-bot`, region: `nrt` (Tokyo) |

---

## Config (Key Constants)

```python
CANDLE_SECONDS       = 60       # 1-minute bars
FIXED_SL_PTS         = 150.0   # Fixed SL distance in points
FIXED_TP_PTS         = 200.0   # Fixed TP distance in points
MIN_BODY_PTS         = 50.0    # Min candle body to qualify as burst
MAX_SIGNAL_AGE_S     = 30      # Stale signal cutoff (seconds)
ENTRY_LIMIT_TIMEOUT_S = 45     # Cancel limit entry after 45s
_AUTO_RESTART_AGE    = 600.0   # 10min stale feed → self-restart
```

---

## Deployment

**Platform:** Fly.io (Tokyo — `nrt` region, co-located with Delta AWS Tokyo)
**App:** `millionare-shivji-1min-bot`
**Volume:** `volsurge_1min_data` → `/data` (persistent trades/logs)
**GitHub:** https://github.com/forjenkins26-sys/Millionaire_1min_bot_Shivji

### Deploy
```bash
git push
flyctl deploy --app millionare-shivji-1min-bot
```

### Check logs
```bash
flyctl logs --app millionare-shivji-1min-bot
```

### SSH into machine
```bash
flyctl ssh console --app millionare-shivji-1min-bot
```

---

## Fly.io Secrets (required)

| Secret | Description |
|---|---|
| `DELTA_API_KEY_LIVE` | Delta Exchange API key |
| `DELTA_API_SECRET_LIVE` | Delta Exchange API secret |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for alerts |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |
| `PAPER_MODE` | `true` = paper, `false` = live orders |
| `LOT_SIZE` | BTC lot size (e.g. `0.001`) |

> **IP Whitelist:** Delta API key must whitelist the Fly.io machine egress IP.
> Current IP: `216.246.19.84` (nrt region).
> If redeployed to new machine, get new IP via `flyctl ssh console -C "curl ifconfig.me"`.

---

## Dashboard Endpoints

| Endpoint | Description |
|---|---|
| `/dashboard` | Full HTML trading dashboard |
| `/health` | JSON health — preflight, WS status, price |
| `/api/live` | Live price + unrealised PnL |
| `/api/stream` | SSE price stream (~200ms) |
| `/api/close` | Manually close open trade |
| `/data` | Download trades CSV |

---

## Important Notes

- **Delta India rejects `stop_market_order`** — SL is enforced by software monitor, not exchange
- **Heikin-Ashi is mandatory** — switching to regular OHLC drops WR from 78% to 49%
- **Signal timeframe stored as `"1"`** in CSV/state — used for audit/reconciliation
- **`client_order_id`** on every order = idempotent — safe to retry on network failure
- **Watchdog** checks every 60s — if no candle for 2min, logs warning; if 10min stale, auto-restarts

---

## Related Bots

| Bot | Folder | App | Timeframe |
|---|---|---|---|
| BTC 5m | `volsurge_5m` | `millionare-shivji-tradingbot` | 5 min |
| BTC 15m | `volsurge_15m` | `millionare-shivji-15m-bot` | 15 min |
| **BTC 1m** | `volSurge_1min` | `millionare-shivji-1min-bot` | **1 min** ← this bot |
| Gold 5m | `Gold mt5_Vol Surge 5 min` | Local MT5/XM | 5 min |
