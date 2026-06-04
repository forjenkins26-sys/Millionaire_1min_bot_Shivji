# Vol Surge v5 — Anand Bot · 1min BTC/USD

## Overview
WebSocket-native live trading bot for **BTCUSD Perpetual** on **Delta Exchange India**.
Bot name: **👤 ANAND BOT**
Detects Vol Surge signals from live 1-minute Heikin-Ashi candles — no TradingView webhook.

---

## Dashboard & Links

| Resource | URL |
|---|---|
| **This bot dashboard** | https://millionare-shivji-1min-bot.fly.dev/dashboard |
| Mummy bot dashboard | https://millionare-shivji-tradingbot.fly.dev/dashboard |
| GitHub repo | https://github.com/forjenkins26-sys/Millionaire_1min_bot_Shivji |

---

## Architecture

```
Delta WebSocket (candlestick_1m)
        ↓
  CandleFeed (candle_feed.py)       ← buffers 300 bars, real-time mark price
        ↓
  _rc_to_ha() / _seed_ha_from_buffer()   ← incremental Heikin-Ashi (Pine-exact)
        ↓
  SignalEngine (signal_engine.py)   ← Vol Surge v5 Pine-parity signal logic
        ↓
  volsurge_v5_live.py               ← entry/exit execution + FastAPI dashboard
        ↓
  Delta Exchange REST API           ← bracket order (SL stop-market + TP limit)
```

---

## Strategy

### Signal: Vol Surge v5 (Pine-parity)
- **Candle type:** Heikin-Ashi (`USE_HA_CANDLES=true`) — mandatory for 78% WR
- **Lookback:** 5 bars (`VS_LOOKBACK=5`)
- **Burst detection:** HA body ≥ `chopAvgTR × 2.0` (`VS_BURST_MULT=2.0`)
- **Min body filter:** 50 pts (`MIN_BODY_PTS=50.0`) — blocks tiny-chop signals
- **Breakout context:** ON, 5 bars (`USE_BREAKOUT_CTX=true`, `BREAKOUT_CTX_BARS=5`) — burst must break prior 5-bar range
- **Cooldown:** 3 bars after signal (`VS_COOLDOWN=3`)
- **Session filter:** OFF — trades 24/7
- **EMA filter:** OFF

### SL/TP Model
| Parameter | Value | Notes |
|---|---|---|
| SL | **15 pts fixed** | Software monitor + exchange bracket stop-market |
| TP | **40 pts fixed** | Exchange bracket limit order (GTC) |
| R:R | ~2.67:1 | TP = 2.67× SL |
| Entry | Breakout stop @ signal HIGH/LOW | BUY: enters only if next bar breaks above signal HA-HIGH |

### Entry Mode: Breakout Stop (matches Pine `pLimit := high`)
- Signal bar closes → bot arms `watch_breakout_entry(trigger=signal_candle.HIGH)`
- BUY entry fires only when `mark_price >= signal_HIGH` (momentum confirmed)
- SELL entry fires only when `mark_price <= signal_LOW`
- Timeout: 120s (`ENTRY_LIMIT_TIMEOUT_S=120`) → skip if no breakout in window
- Stale guard: skip if bar closed >150s ago (`MAX_SIGNAL_AGE_S=150`)

### Exit
- **TP:** Exchange bracket limit order — fills automatically when price hits TP
- **SL:** Exchange bracket stop-market (survives bot crash) + software monitor backup
- **Monitor:** Wakes on every WS mark_price tick (~50ms) — stale-price sanity guard (reject >500pt jumps)
- **Manual:** `/api/close` or dashboard "Close Trade" button

---

## Files

| File | Purpose |
|---|---|
| `volsurge_v5_live.py` | Main bot — signal handling, order execution, FastAPI dashboard |
| `candle_feed.py` | Delta WebSocket 1m candle feed + REST backfill (300 bars) |
| `signal_engine.py` | Vol Surge v5 signal logic — Pine-parity, timeframe-agnostic |
| `private_ws.py` | Delta private WebSocket — instant fill detection via orders/trades channels |
| `fly.toml` | Fly.io deploy config — env vars, volume mount, health check |
| `Dockerfile` | Docker image — python:3.11-slim + uvicorn |
| `requirements_v5.txt` | Python deps |
| `docs/pine_volsurge_v5.pine` | TradingView Pine script — visual reference + journal (SL=15, TP=40) |

---

## Live Config (fly.toml [env])

```
CANDLE_SECONDS        = 60        # 1-minute bars
FIXED_SL_PTS          = 15.0      # Fixed SL distance in points
FIXED_TP_PTS          = 40.0      # Fixed TP distance in points
MIN_BODY_PTS          = 50.0      # Min HA body to qualify as burst
USE_BREAKOUT_CTX      = true      # Burst must break prior 5-bar range
BREAKOUT_CTX_BARS     = 5         # Range lookback bars
USE_LIMIT_ENTRY       = true      # Breakout stop entry (not market)
ENTRY_LIMIT_TIMEOUT_S = 120       # Wait up to 120s for breakout
MAX_SIGNAL_AGE_S      = 150       # Skip stale signals >150s old
VS_BURST_MULT         = 2.0       # Burst multiplier (fly.toml)
SL_MULT               = 1.8       # From .env (used only if FIXED_SL_PTS=0)
USE_HA_CANDLES        = true      # From .env
VS_LOOKBACK           = 5         # From .env
VS_COOLDOWN           = 3         # From .env
```

---

## Deployment

**Platform:** Fly.io — `nrt` region (Tokyo, co-located with Delta AWS Tokyo)
**App name:** `millionare-shivji-1min-bot`
**Volume:** `volsurge_1min_data` → `/data` (persistent trades + logs)
**Auto-deploy:** GitHub push → Fly.io fetches and redeploys automatically

```bash
# Manual deploy
flyctl deploy --app millionare-shivji-1min-bot

# Logs
flyctl logs --app millionare-shivji-1min-bot

# SSH
flyctl ssh console --app millionare-shivji-1min-bot

# Get machine IP (for Delta API whitelist)
flyctl ssh console -C "curl ifconfig.me" --app millionare-shivji-1min-bot
```

---

## Fly.io Secrets Required

| Secret | Description |
|---|---|
| `DELTA_API_KEY_LIVE` | Delta Exchange live API key |
| `DELTA_API_SECRET_LIVE` | Delta Exchange live API secret |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |
| `PAPER_MODE` | `false` for live, `true` for paper |
| `LOT_SIZE` | BTC lot size (`0.001`) |

> **IP Whitelist:** Delta API key must whitelist Fly.io machine egress IP.
> Current IP: `216.246.19.84` (nrt region).
> On redeploy to new machine, get new IP via `flyctl ssh console -C "curl ifconfig.me"`.

---

## Dashboard Endpoints

| Endpoint | Description |
|---|---|
| `/dashboard` | Full HTML trading dashboard |
| `/health` | JSON — preflight status, WS feed health, current price |
| `/api/live` | Live price + unrealised PnL |
| `/api/stream` | SSE price stream (~200ms) |
| `/api/close` | Manually close open trade |
| `/data` | Download trades CSV |
| `/preflight` | Run pre-flight validation checks |

---

## Important Notes

- **Delta India rejects `stop_market_order`** — SL enforced by software monitor + bracket order
- **Heikin-Ashi mandatory** — regular OHLC gives 49% WR vs 78% WR on HA
- **Bracket order** — single `/v2/orders/bracket` call places both SL stop + TP limit server-side; survives bot crash
- **Stale-price guard** — position monitor rejects WS price jumps >500pts (prevents false SL on reconnect)
- **Breakout entry** — bot only enters when signal candle HIGH/LOW is broken by next bar; matches Pine `[BREAKOUT@HIGH/LOW]`
- **`breakout_trigger_px`** — slippage is measured vs signal HIGH/LOW (breakout level), not HA-close
- **Pine parity** — `signal_engine.py` implements exact Pine v5 math: HA conversion, Wilder RMA ATR, chop avg TR, burst detection

---

## Known Issues / Resolved

| Date | Issue | Status |
|---|---|---|
| 04-Jun-2026 | `NameError: breakout_trigger_px not defined` in `_set_open_trade` | ✅ Fixed — added param, pushed commit `3f93efa` |
| 04-Jun-2026 | Breakout trigger used real candle HIGH/LOW instead of HA candle HIGH/LOW | ✅ Fixed — `_breakout_px = ha_candle.high/low` (Pine: `pLimit := high` on HA chart = HA high, not real high) |

---

## Future Improvements (Backtest-Validated)

Two configs from 1-year parameter sweep (May 2025–May 2026) passed >60% WR with 0 losing months:

| Config | SL | TP | MinBody | BurstMult | Cooldown | WR | Trades/yr |
|---|---|---|---|---|---|---|---|
| TC6 ⭐ | 100 | 200 | 100 pts | 2.5 | 3 | 62% | 869 |
| TC10 | 100 | 200 | 100 pts | 3.0 | 5 | 65% | 491 |

Current live config (SL=15, TP=40, body=50, burst=2.0) is conservative/tight — good for low-risk validation phase.

---

## Related Bots

| Bot | Folder | App | Timeframe |
|---|---|---|---|
| 🤖 Mummy | `volsurge_1m_Mummy_Live` | `millionare-shivji-tradingbot` | 1 min |
| **👤 Anand (this)** | `volSurge_1min_Anand_Live` | `millionare-shivji-1min-bot` | **1 min** |

---

## Changelog

### 04-Jun-2026 ~13:00 IST
**Bug fix: `breakout_trigger_px` NameError crash on every live entry**

- **Problem:** `_set_open_trade()` referenced `breakout_trigger_px` as free variable — not in its parameter list. Caused `NameError` on every trade entry. Position filled on Delta Exchange but bot state never set → position monitor never started → orphan position.
- **Root cause:** `_process_entry()` had `breakout_trigger_px` as local param but never passed it to `_set_open_trade()`.
- **Fix:** Added `breakout_trigger_px: float = 0.0` to `_set_open_trade()` signature. Added `breakout_trigger_px=breakout_trigger_px` in call from `_process_entry()`.
- **Files changed:** `volsurge_v5_live.py`
- **Commit:** `3f93efa` — pushed to `origin/master`
- **Deploy:** Auto-deployed via Fly.io GitHub integration

### 04-Jun-2026 ~14:30 IST
**Bug fix: Breakout trigger used real candle HIGH/LOW instead of HA HIGH/LOW**

- **Problem:** `_breakout_px = candle.high` used real candle's high as BUY trigger. Pine uses `pLimit := high` on HA chart → trigger = HA candle HIGH. HA high = max(real_high, ha_open, ha_close) ≥ real_high. Using real high = lower trigger = bot entered earlier/more often than Pine intends.
- **Fix:** Changed to `_breakout_px = ha_candle.high if BUY else ha_candle.low`
- **File:** `volsurge_v5_live.py` line ~1944
- **Impact:** Bot now matches Pine's breakout trigger exactly. Slightly fewer fills (harder trigger) but higher quality entries.

### 04-Jun-2026 ~13:15 IST
**Documentation: Updated Project_Understanding.md**

- Corrected SL/TP from stale values (150/200) to actual live values (15/40 per fly.toml)
- Corrected entry mode description from "market order" to "breakout stop at signal HIGH/LOW"
- Added full live config table from fly.toml
- Added changelog section
- Added known issues table

<!-- deploy-test: 04-Jun-2026 13:59 IST -->
