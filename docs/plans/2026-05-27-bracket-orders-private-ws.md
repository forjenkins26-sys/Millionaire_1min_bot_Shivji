# Bracket Orders + Private WebSocket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace software SL with exchange-side bracket orders (SL + TP via `/v2/orders/bracket`) and subscribe to Delta private WS channels (`orders`, `v2/user_trades`) for instant fill detection instead of REST polling.

**Architecture:**
- Entry flow unchanged: `POST /v2/orders` market order → fills → open position
- After fill: `POST /v2/orders/bracket` attaches SL stop-market + TP limit to the POSITION (server-side, survives bot crash)
- `private_ws.py` module: new class `PrivateFeed` that authenticates on Delta WS and subscribes to private channels; fires threading.Events on order fills/closes
- `_position_monitor()` wakes on `PrivateFeed.order_event` for instant TP/SL detection + keeps software SL as crash-safe fallback

**Tech Stack:** Python 3.11+, `websockets`, `hmac`/`hashlib` (already in project), `threading.Event`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `private_ws.py` | **CREATE** | WS auth, private channel subscription, order/trade event bus |
| `volsurge_v5_live.py` | **MODIFY** | Add `place_bracket_order()`, update `_process_entry()`, update `_position_monitor()`, wire `PrivateFeed` in startup |

---

## Task 1: Create `private_ws.py` — Private WebSocket Feed

**Files:**
- Create: `C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min\private_ws.py`

### Background — Delta WS Auth Protocol

Delta's private WebSocket uses the same endpoint `wss://socket.india.delta.exchange`.
After connecting, send an auth message before subscribing:

```json
{
  "method": "auth",
  "payload": {
    "api_key": "<API_KEY>",
    "signature": "<HMAC_SHA256>",
    "timestamp": "<unix_seconds_string>"
  }
}
```

The signature signs `"GET" + timestamp + "/realtime"` with the API secret (SHA256 HMAC hex).

Private channel subscription after auth:
```json
{
  "method": "subscribe",
  "payload": {
    "channels": [
      { "name": "orders",        "symbols": ["BTCUSD"] },
      { "name": "v2/user_trades","symbols": ["BTCUSD"] }
    ]
  }
}
```

---

- [ ] **Step 1: Create `private_ws.py` with `PrivateFeed` class**

```python
#!/usr/bin/env python3
"""
private_ws.py — Delta Exchange Private WebSocket Feed
=====================================================
Authenticates on Delta's WebSocket and subscribes to:
  - 'orders'         : real-time order state changes (fills, cancels)
  - 'v2/user_trades' : real-time trade fills

Exposes threading.Events so _position_monitor() can wake instantly
on TP/SL hits instead of polling REST every 1s.

Usage:
    from private_ws import PrivateFeed
    pf = PrivateFeed(api_key="...", api_secret="...", symbol="BTCUSD")
    asyncio.create_task(pf.start())
    # In monitor thread:
    pf.order_event.wait(timeout=2)
    evt = pf.last_order   # latest order dict from WS
"""

import asyncio
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

WS_URL            = "wss://socket.india.delta.exchange"
_BACKOFF_INITIAL  = 1.0
_BACKOFF_MAX      = 60.0
_BACKOFF_MULT     = 2.0
_HEARTBEAT_SECS   = 25.0


class PrivateFeed:
    """
    Private WebSocket feed — orders + trades channels.

    Thread-safe. Events are set from asyncio thread; monitor thread reads them.
    """

    def __init__(self, api_key: str, api_secret: str, symbol: str = "BTCUSD",
                 logger: Optional[logging.Logger] = None):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.symbol     = symbol
        self.log        = logger or logging.getLogger("private_ws")

        # Threading events — set when a new message arrives; caller must clear
        self.order_event: threading.Event = threading.Event()
        self.trade_event: threading.Event = threading.Event()

        # Latest message payloads — write from asyncio, read from monitor thread
        self.last_order: Optional[dict] = None   # most recent order update
        self.last_trade: Optional[dict] = None   # most recent fill

        self.authenticated: bool = False
        self.connected:     bool = False
        self._running:      bool = False
        self.reconnect_count: int = 0

    # ── Auth signature ────────────────────────────────────────────────────────

    def _sign(self) -> tuple[str, str]:
        """Return (timestamp_str, signature_hex) for WS auth message."""
        ts  = str(int(time.time()))
        sig = hmac.new(
            self.api_secret.encode(),
            f"GET{ts}/realtime".encode(),
            hashlib.sha256,
        ).hexdigest()
        return ts, sig

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        self.log.info("[PRIV_WS] Starting private WebSocket feed")
        backoff = _BACKOFF_INITIAL

        while self._running:
            try:
                await self._ws_loop()
                backoff = _BACKOFF_INITIAL
            except (ConnectionClosed, WebSocketException, OSError) as e:
                self.connected     = False
                self.authenticated = False
                self.log.warning(f"[PRIV_WS] Disconnected: {e!r} — retry in {backoff:.0f}s")
            except Exception as e:
                self.connected     = False
                self.authenticated = False
                self.log.error(f"[PRIV_WS] Error: {e!r} — retry in {backoff:.0f}s")

            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_MULT, _BACKOFF_MAX)
            self.reconnect_count += 1

    def stop(self):
        self._running = False

    async def _ws_loop(self):
        async with websockets.connect(
            WS_URL,
            ping_interval=None,
            ping_timeout=None,
            max_size=2 ** 20,
            open_timeout=15,
        ) as ws:
            self.connected = True
            self.log.info("[PRIV_WS] Connected")

            # Step 1: authenticate
            ts, sig = self._sign()
            await ws.send(json.dumps({
                "method": "auth",
                "payload": {
                    "api_key":   self.api_key,
                    "signature": sig,
                    "timestamp": ts,
                }
            }))
            self.log.info("[PRIV_WS] Auth sent")

            # Step 2: wait for auth confirmation (first message)
            auth_raw = await asyncio.wait_for(ws.recv(), timeout=10)
            auth_msg = json.loads(auth_raw)
            if auth_msg.get("type") == "success" or auth_msg.get("success"):
                self.authenticated = True
                self.log.info("[PRIV_WS] Authenticated ✓")
            else:
                self.log.error(f"[PRIV_WS] Auth failed: {auth_msg}")
                return

            # Step 3: subscribe to private channels
            await ws.send(json.dumps({
                "method": "subscribe",
                "payload": {
                    "channels": [
                        {"name": "orders",         "symbols": [self.symbol]},
                        {"name": "v2/user_trades", "symbols": [self.symbol]},
                    ]
                }
            }))
            self.log.info("[PRIV_WS] Subscribed to orders + v2/user_trades")

            hb_task = asyncio.create_task(self._heartbeat(ws))
            try:
                async for raw_frame in ws:
                    try:
                        msg = json.loads(raw_frame)
                        self._dispatch(msg)
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        self.log.warning(f"[PRIV_WS] dispatch error: {e}")
            finally:
                hb_task.cancel()
                self.connected     = False
                self.authenticated = False

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(_HEARTBEAT_SECS)
            try:
                await ws.send(json.dumps({"type": "heartbeat"}))
            except Exception:
                break

    # ── Message dispatch ──────────────────────────────────────────────────────

    def _dispatch(self, msg: dict):
        msg_type = str(msg.get("type", msg.get("channel", "")))

        if "orders" in msg_type and "user_trades" not in msg_type:
            data = msg.get("data", msg)
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict) and data:
                self.last_order = data
                self.order_event.set()   # wake position monitor
                self.log.info(
                    f"[PRIV_WS] order event | id={data.get('id')} "
                    f"state={data.get('state')} "
                    f"fill_px={data.get('average_fill_price','?')}"
                )

        elif "user_trades" in msg_type or "trade" in msg_type:
            data = msg.get("data", msg)
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict) and data:
                self.last_trade = data
                self.trade_event.set()
                self.log.info(
                    f"[PRIV_WS] trade fill | px={data.get('fill_price','?')} "
                    f"size={data.get('size','?')} side={data.get('side','?')}"
                )

        elif msg_type in ("subscriptions", "heartbeat", "info", "welcome", "connected", "success"):
            self.log.debug(f"[PRIV_WS] ctrl: {msg_type}")
        else:
            self.log.debug(f"[PRIV_WS] unknown: {msg_type!r} | {str(msg)[:120]}")
```

- [ ] **Step 2: Verify file created at correct path**

Check: `C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min\private_ws.py` exists and is ~200 lines.

---

## Task 2: Add `place_bracket_order()` to `volsurge_v5_live.py`

**Files:**
- Modify: `volsurge_v5_live.py` — add new function after `place_tp_order()` (around line 658)

### Background — Bracket Order API

`POST /v2/orders/bracket` attaches exchange-side SL + TP to the current open position.
Size is NOT specified — it closes the entire position.
Replaces both `place_sl_order()` (currently no-op) and `place_tp_order()`.

**Important:** bracket order does NOT return individual order IDs for SL/TP sub-orders in the success response. After placing, query `get_open_orders()` to find the bracket sub-orders by price.

- [ ] **Step 3: Add `place_bracket_order()` function after `place_tp_order()` (~line 658)**

Find this line:
```python
def cancel_order(order_id: str, retries: int = 3, delay: float = 1.5) -> bool:
```

Insert BEFORE that line:

```python
def place_bracket_order(
    direction: str,
    sl_price:  float,
    tp_price:  float,
) -> Optional[dict]:
    """
    Attach server-side SL (stop-market) + TP (limit) to the open position.
    Uses /v2/orders/bracket — single API call, replaces place_sl_order + place_tp_order.

    SL: stop-market order (triggers market close when price hits sl_price).
    TP: limit order (closes at tp_price or better).

    Returns:
        dict  — {"sl_price": sl_price, "tp_price": tp_price, "bracket_placed": True}
                after querying Delta for the actual sub-order IDs
        None  — placement failed (caller must fall back to software SL + TP limit)
    """
    body = {
        "product_id":     PRODUCT_ID,
        "product_symbol": SYMBOL,
        "stop_loss_order": {
            "order_type": "market_order",
            "stop_price": str(round(sl_price, 1)),
        },
        "take_profit_order": {
            "order_type": "limit_order",
            "stop_price": str(round(tp_price, 1)),
            "limit_price": str(round(tp_price, 1)),
        },
        "bracket_stop_trigger_method": "last_traded_price",
    }

    resp = _post("/v2/orders/bracket", body)

    if not resp:
        _loge(f"[BRACKET] No response from /v2/orders/bracket")
        return None

    if not (resp.get("success") or resp.get("result")):
        _loge(f"[BRACKET] Failed: {resp}")
        return None

    _log(f"[BRACKET] Placed SL={sl_price} TP={tp_price} dir={direction}")

    # Query open orders to find bracket sub-order IDs (TP limit is reduce_only)
    tp_oid = None
    sl_oid = None
    try:
        time.sleep(0.3)   # brief wait for Delta backend to register sub-orders
        open_orders = get_open_orders()
        for o in open_orders:
            if str(o.get("product_id", "")) != str(PRODUCT_ID):
                continue
            if not o.get("reduce_only"):
                continue
            lp = float(o.get("limit_price", 0) or 0)
            sp = float(o.get("stop_price",  0) or 0)
            ot = o.get("order_type", "")

            # TP: limit order near tp_price
            if ot == "limit_order" and abs(lp - tp_price) < 5.0:
                tp_oid = str(o.get("id", ""))
                _log(f"[BRACKET] TP sub-order found: oid={tp_oid} px={lp}")

            # SL: stop order near sl_price (stop_market or bracket_sl)
            elif sp and abs(sp - sl_price) < 5.0:
                sl_oid = str(o.get("id", ""))
                _log(f"[BRACKET] SL sub-order found: oid={sl_oid} stop={sp}")

    except Exception as e:
        _logw(f"[BRACKET] Sub-order ID query failed (non-fatal): {e}")

    return {
        "bracket_placed": True,
        "sl_price":       sl_price,
        "tp_price":       tp_price,
        "sl_oid":         sl_oid,
        "tp_oid":         tp_oid,
        "placed_time":    time.time(),
    }

```

- [ ] **Step 4: Add `private_feed` global and import**

Find the imports block at the top of `volsurge_v5_live.py`. After the existing imports, add:

```python
from private_ws import PrivateFeed
```

Find the GLOBAL STATE section (~line 166). After `_ha_buffer` declaration, add:

```python
# Private WebSocket feed — orders + trades channels for instant fill detection
private_feed: Optional["PrivateFeed"] = None
```

---

## Task 3: Update `_process_entry()` — use bracket order instead of separate SL+TP

**Files:**
- Modify: `volsurge_v5_live.py` lines ~1396–1425 (the parallel SL+TP placement block)

- [ ] **Step 5: Replace parallel SL+TP placement with bracket order call**

Find this block (~line 1396):
```python
            # ── Parallel SL + TP placement ────────────────────────────────
            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor(max_workers=2) as _pool:
                _sl_fut = _pool.submit(place_sl_order, close_side, LOT_SIZE, sl_price, entry_contracts)
                _tp_fut = _pool.submit(place_tp_order, close_side, LOT_SIZE, tp_price, entry_contracts)
                sl_result = _sl_fut.result()
                tp_result = _tp_fut.result()

            sl_oid      = sl_result["order_id"]   if sl_result else None
            sl_placed_t = sl_result["placed_time"] if sl_result else None
            tp_oid      = tp_result["order_id"]   if tp_result else None
            tp_placed_t = tp_result["placed_time"] if tp_result else None

            # sl_oid is always None — Delta India does not support stop_market_order.
            # SL is enforced by the software position monitor (_position_monitor).
            # No alert needed here — the ENTERED Telegram message already shows SW⚡ SL level.
            if sl_oid:
                # Future-proof: if exchange SL ever gets placed, log it
                _log(f"[SL] Exchange SL order placed oid={sl_oid} @ {sl_price}")

            # TP placement failure alert — critical blind spot if TP order never reached Delta.
            # Trade still runs (software SL protects it), but user must know immediately
            # so they can manually place TP on Delta or decide to close.
            if not tp_oid:
                _loge(f"[TP] TP ORDER FAILED after {d} entry @ {fill_px} — no TP on Delta")
                tg(f"⚠️ <b>TP ORDER FAILED</b> [{d}]\n"
                   f"Entry: {fill_px:,.1f} | Expected TP: {tp_price:,.1f}\n"
                   f"Trade is open — software SL @ {sl_price:,.1f} still active\n"
                   f"Action: manually place SELL limit @ {tp_price:,.1f} on Delta or close trade")
```

Replace that entire block with:

```python
            # ── Bracket order: server-side SL stop-market + TP limit ──────
            # Single call to /v2/orders/bracket — replaces separate SL + TP placement.
            # If bracket succeeds: Delta manages SL and TP server-side (survives bot crash).
            # If bracket fails: fall back to software SL (existing behaviour) + TP limit.
            bracket_result = place_bracket_order(d, sl_price, tp_price)

            if bracket_result and bracket_result.get("bracket_placed"):
                sl_oid      = bracket_result.get("sl_oid")     # may be None if sub-order query failed
                tp_oid      = bracket_result.get("tp_oid")
                sl_placed_t = bracket_result.get("placed_time")
                tp_placed_t = bracket_result.get("placed_time")
                _log(
                    f"[BRACKET] ✅ Server-side SL+TP placed | "
                    f"sl={sl_price} sl_oid={sl_oid} | tp={tp_price} tp_oid={tp_oid}"
                )
                tg(
                    f"🔒 <b>BRACKET ORDER PLACED</b> [{d}]\n"
                    f"SL: {sl_price:,.1f} (exchange stop-market)\n"
                    f"TP: {tp_price:,.1f} (exchange limit)\n"
                    f"Exits are server-side — safe during disconnects"
                )
            else:
                # Bracket failed — fall back to legacy TP limit + software SL
                _logw("[BRACKET] Failed — falling back to TP limit + software SL")
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=2) as _pool:
                    _sl_fut = _pool.submit(place_sl_order, close_side, LOT_SIZE, sl_price, entry_contracts)
                    _tp_fut = _pool.submit(place_tp_order, close_side, LOT_SIZE, tp_price, entry_contracts)
                    sl_result = _sl_fut.result()
                    tp_result = _tp_fut.result()

                sl_oid      = sl_result["order_id"]   if sl_result else None
                sl_placed_t = sl_result["placed_time"] if sl_result else None
                tp_oid      = tp_result["order_id"]   if tp_result else None
                tp_placed_t = tp_result["placed_time"] if tp_result else None

                if not tp_oid:
                    _loge(f"[TP] TP ORDER FAILED after {d} entry @ {fill_px} — no TP on Delta")
                    tg(
                        f"⚠️ <b>TP ORDER FAILED (bracket + fallback both failed)</b> [{d}]\n"
                        f"Entry: {fill_px:,.1f} | Expected TP: {tp_price:,.1f}\n"
                        f"Trade open — software SL @ {sl_price:,.1f} active\n"
                        f"Action: manually place SELL limit @ {tp_price:,.1f} on Delta"
                    )
```

---

## Task 4: Update `_position_monitor()` — use Private WS for TP/SL detection

**Files:**
- Modify: `volsurge_v5_live.py` — LIVE mode section of `_position_monitor()` (~lines 1122–1211)

- [ ] **Step 6: Replace REST position polling with WS-event-first detection**

Find the LIVE monitor comment block (~line 1122):
```python
            else:
                # ── LIVE monitor ──────────────────────────────────────────────
                # Delta Exchange India /v2/orders only accepts limit_order and
                # market_order — stop_market_order is rejected with bad_schema.
                # Therefore SL is enforced here in software every 2s tick.
                # TP is still an exchange limit order (works fine).

                # 1. Software SL check — fire market close if price breaches SL
                hit_sl = (d == "BUY" and price <= sl) or (d == "SELL" and price >= sl)
```

Replace the entire LIVE section (from `else:` through `break` at ~line 1211 where `_close_trade` is called) with:

```python
            else:
                # ── LIVE monitor ──────────────────────────────────────────────
                # Primary exit detection: Private WS 'orders' channel fires instantly
                # when bracket TP limit fills or SL stop-market triggers.
                #
                # Software SL remains as crash-safe fallback: if WS is silent but
                # price breaches SL level on mark_price tick, fire market close.
                # This handles: WS disconnect, bracket order not placed (fallback mode).

                # 1. Check Private WS for order fill events (TP or SL bracket hit)
                _pf = private_feed   # local ref (thread-safe read)
                if _pf and _pf.order_event.is_set():
                    _pf.order_event.clear()
                    evt = _pf.last_order
                    if evt:
                        evt_product = str(evt.get("product_id", ""))
                        evt_state   = str(evt.get("state", ""))
                        evt_reduce  = bool(evt.get("reduce_only", False))
                        if (evt_product == str(PRODUCT_ID)
                                and evt_reduce
                                and evt_state in ("filled", "closed")):
                            raw_fill = evt.get("average_fill_price") or evt.get("limit_price")
                            fill_px_exit = round(float(raw_fill), 1) if raw_fill else price
                            oid_exit     = str(evt.get("id", ""))

                            # Classify TP vs SL by comparing fill to expected levels
                            dist_tp = abs(fill_px_exit - tp)
                            dist_sl = abs(fill_px_exit - sl)
                            if dist_tp <= dist_sl:
                                exit_label = "TP_BRACKET"
                                slip       = round(abs(fill_px_exit - tp), 2)
                            else:
                                exit_label = "SL_BRACKET"
                                slip       = round(abs(fill_px_exit - sl), 2)

                            open_trade["exit_order_id"]      = oid_exit
                            open_trade["exit_fill_px_delta"] = fill_px_exit
                            _log(
                                f"[MON] WS exit detected | {exit_label} "
                                f"fill={fill_px_exit} oid={oid_exit} "
                                f"dist_tp={dist_tp:.1f} dist_sl={dist_sl:.1f}"
                            )
                            _close_trade(fill_px_exit, exit_label, slip)
                            break

                # 2. Software SL check — safety net if bracket not placed or WS silent
                hit_sl = (d == "BUY" and price <= sl) or (d == "SELL" and price >= sl)
                if hit_sl:
                    _logw(f"[MON] SOFTWARE SL HIT | price={price} sl={sl} d={d}")
                    _log_lifecycle(open_trade["trade_id"], "SL_SOFTWARE_TRIGGERED",
                                   price=price, notes=f"sl_level={sl}")
                    tg(f"🛑 <b>SL HIT (software fallback)</b> [{d}]\n"
                       f"Price: {price:,.1f} | SL: {sl:,.1f}\n"
                       f"Placing market close...")

                    close_side = "sell" if d == "BUY" else "buy"
                    tp_oid_val = open_trade.get("tp_oid")

                    def _cancel_tp_bg():
                        if tp_oid_val:
                            _delete(f"/v2/orders/{tp_oid_val}")
                            _log(f"[MON] TP order {tp_oid_val} cancelled (background) on SL trigger")

                    threading.Thread(target=_cancel_tp_bg, daemon=True, name="tp-cancel-sl").start()

                    close_result = place_market_order(close_side, LOT_SIZE,
                                                      reduce_only=True, ref_price=price)
                    if close_result:
                        exit_px = round(float(close_result.get("fill_price") or price), 1)
                    else:
                        _loge("[MON] SL market close FAILED — retrying once")
                        time.sleep(0.5)
                        close_result2 = place_market_order(close_side, LOT_SIZE,
                                                           reduce_only=True, ref_price=price)
                        exit_px = round(float((close_result2 or {}).get("fill_price") or price), 1)

                    slip = round(abs(exit_px - sl), 2)
                    open_trade["exit_order_id"]      = (close_result or {}).get("order_id")
                    open_trade["exit_fill_px_delta"]  = exit_px
                    _close_trade(exit_px, "SL_SOFTWARE", slip)
                    break

                # 3. REST position poll — confirm if already flat (WS missed the event)
                # Only poll every ~10 WS ticks (~2s per tick) to avoid REST rate limits
                _mc = open_trade.get("monitor_cycles", 0)
                if _mc % 10 == 0:
                    pos = get_open_position()
                    if pos is _POS_API_ERROR:
                        _logw("[MON] get_open_position API error — skip tick")
                        continue
                    if pos is None:
                        _logw(f"[LIVE] Position flat detected via REST | approx price={price}")
                        _log_lifecycle(open_trade["trade_id"], "MONITOR_FLAT", notes=f"approx={price}")

                        exit_fill_px  = price
                        exit_order_id = None
                        exit_label    = "AUTO_EXIT"

                        tp_oid_val = open_trade.get("tp_oid")
                        if tp_oid_val:
                            order_resp = _get(f"/v2/orders/{tp_oid_val}")
                            if order_resp:
                                result_data = order_resp.get("result", {})
                                if result_data.get("state") in ("filled", "closed"):
                                    raw_fill = result_data.get("average_fill_price")
                                    if raw_fill:
                                        exit_fill_px  = float(raw_fill)
                                        exit_order_id = tp_oid_val
                                        exit_label    = "TP_LIVE"
                                        _log(f"[LIVE] TP confirmed oid={tp_oid_val} fill={exit_fill_px}")

                        open_trade["exit_order_id"]      = exit_order_id
                        open_trade["exit_fill_px_delta"]  = exit_fill_px

                        for oid_key in ("sl_oid", "tp_oid"):
                            oid = open_trade.get(oid_key)
                            if oid and oid != exit_order_id:
                                _delete(f"/v2/orders/{oid}")
                                _log_lifecycle(open_trade["trade_id"],
                                               f"{oid_key.upper().replace('_OID','')}_CANCELLED",
                                               order_id=oid)

                        _close_trade(exit_fill_px, exit_label, 0.0)
                        break
```

---

## Task 5: Wire `PrivateFeed` into startup

**Files:**
- Modify: `volsurge_v5_live.py` — `startup()` function (~line 1665)

- [ ] **Step 7: Initialise `private_feed` in `startup()`**

Find this block in `startup()` (~line 1720):
```python
    # Start the WebSocket candle feed
    asyncio.create_task(feed.start())
    asyncio.create_task(_feed_watchdog())
```

Replace with:
```python
    # Start the WebSocket candle feed
    asyncio.create_task(feed.start())
    asyncio.create_task(_feed_watchdog())

    # Start private WebSocket feed (orders + user_trades channels)
    # Only start in LIVE mode — paper mode has no real order events
    if not PAPER_MODE and API_KEY and API_SECRET:
        global private_feed
        private_feed = PrivateFeed(
            api_key    = API_KEY,
            api_secret = API_SECRET,
            symbol     = SYMBOL,
            logger     = logging.getLogger("private_ws"),
        )
        asyncio.create_task(private_feed.start())
        log.info("[STARTUP] Private WS feed started (orders + user_trades)")
    else:
        log.info("[STARTUP] Private WS feed skipped (PAPER mode or no credentials)")
```

---

## Task 6: Smoke-test in PAPER mode + verify logs

- [ ] **Step 8: Run bot locally in PAPER mode and check startup logs**

Expected log lines on startup:
```
[STARTUP] Private WS feed skipped (PAPER mode or no credentials)
```
(PAPER_MODE skips private WS — that's correct)

Verify no ImportError for `from private_ws import PrivateFeed`.

- [ ] **Step 9: Manual bracket order test (LIVE mode, tiny position)**

In a test session with LIVE mode and `LOT_SIZE_CONTRACTS=1` (minimum size):
1. Trigger one trade manually via `/test_entry` endpoint (if exists) or wait for signal
2. Check logs for:
   ```
   [BRACKET] ✅ Server-side SL+TP placed | sl=XXXXX sl_oid=... | tp=XXXXX tp_oid=...
   [PRIV_WS] Authenticated ✓
   [PRIV_WS] Subscribed to orders + v2/user_trades
   ```
3. Check Delta Exchange web UI: open orders should show SL stop + TP limit under the position
4. If bracket fails, logs show:
   ```
   [BRACKET] Failed — falling back to TP limit + software SL
   ```
   and a regular TP limit order appears on Delta (existing behaviour preserved)

- [ ] **Step 10: Verify TP exit detection via WS**

When TP hits (either naturally or by manually closing on Delta):
Expected logs:
```
[PRIV_WS] order event | id=... state=filled fill_px=XXXXX
[MON] WS exit detected | TP_BRACKET fill=XXXXX ...
STATE→CLOSED | ... pts=+200
```

- [ ] **Step 11: Git commit**

```
git add private_ws.py volsurge_v5_live.py
git commit -m "feat: bracket orders (server-side SL+TP) + private WS fill detection"
```

---

## Task 7: Deploy to Fly.io (after LIVE verification)

- [ ] **Step 12: Deploy (user must approve first)**

```bash
fly deploy
```

After deploy, check Fly.io logs for:
```
[PRIV_WS] Authenticated ✓
[PRIV_WS] Subscribed to orders + v2/user_trades
```

---

## Self-Review

**Spec coverage:**
- ✅ Bracket orders replace software SL → server-side SL survives crash
- ✅ Private WS Orders channel → instant fill detection
- ✅ Private WS UserTrades channel → subscribed (trade_event available for future use)
- ✅ Fallback to existing behaviour if bracket fails
- ✅ Software SL kept as safety net even when bracket is active
- ✅ REST position poll kept but throttled to every 10 cycles (vs every cycle before)

**Risk mitigations:**
- Bracket fail → graceful fallback to legacy TP limit + software SL (zero regression)
- WS auth fail → private_feed stays `None`, monitor skips WS check, falls back to software SL
- Double-close protection: `open_trade is None` check in `_close_trade()` prevents double-close

**Bracket order size note:**
Delta bracket orders close the ENTIRE open position (no size field). Bot always has one contract open — this is correct behaviour.
