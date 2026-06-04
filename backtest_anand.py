#!/usr/bin/env python3
"""
backtest_anand.py — Vol Surge v5 Backtest Engine (Anand Bot Config)
====================================================================
RICE-POT Implementation
-----------------------
R : Quantitative trading systems engineer, 15yr crypto algo backtesting.
I : Exact Pine-parity signal detection via signal_engine.py. HA batch-converted
    once (not per-bar). Breakout entry simulation. Intrabar SL/TP on real OHLC.
    Zero lookahead bias. 300-bar warmup before backtest window.
C : Anand config: SL=15pts, TP=40pts, MIN_BODY=50, breakout_ctx=5bars,
    burst_mult=2.0, lookback=5, cooldown=3, HA=true, entry_valid_bars=2.
E : Uses signal_engine.compute_indicators() on pre-computed HA candles.
P : No lookahead. Real OHLC for exit. Same-bar conflict: TP wins if open gap up,
    else SL wins (conservative). Taker fee 0.05% per side.
O : Console summary + backtest_anand_trades.csv
T : Production Python, type hints, no shortcuts.

Usage:
    # Step 1: build 1-year candle cache (run once, ~3 min)
    python build_cache.py

    # Step 2: run backtest
    python backtest_anand.py
    python backtest_anand.py --days 180   # shorter window
    python backtest_anand.py --no-fee     # ignore commission
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── Local imports (same folder) ───────────────────────────────────────────────
try:
    from candle_feed import Candle
    from signal_engine import (
        SignalConfig,
        compute_indicators,
        compute_ha_candles,
        IndicatorState,
    )
except ImportError as e:
    print(f"[ERROR] Import failed: {e}")
    print("  Run this script from the volSurge_1min_Anand_Live folder.")
    sys.exit(1)

# ════════════════════════════════════════════════════════════════════════════════
# CONFIG  — mirrors Anand bot's fly.toml [env]
# ════════════════════════════════════════════════════════════════════════════════
FIXED_SL_PTS       = 15.0      # FIXED_SL_PTS in fly.toml
FIXED_TP_PTS       = 40.0      # FIXED_TP_PTS in fly.toml
ENTRY_VALID_BARS   = 2         # Pine limitValidBars=2 — breakout must happen within N bars
TAKER_FEE_PCT      = 0.0005    # 0.05% per side (Delta taker fee)

WARMUP_BARS        = 300       # bars before backtest window — seeds HA + EMA
BUFFER_SIZE        = 300       # rolling window passed to compute_indicators()

# Signal engine config — mirrors Anand fly.toml + .env
SIG_CFG = SignalConfig(
    lookback          = 5,
    burst_mult        = 2.0,
    sl_mult           = 1.8,
    cooldown          = 3,
    use_ema_filter    = False,
    use_session       = False,
    safety_factor     = 1.0,
    use_ha            = False,   # HA already pre-applied — pass HA candles as raw
    use_min_body      = True,
    min_body_pts      = 50.0,
    use_breakout_ctx  = True,
    breakout_ctx_bars = 5,
)

IST = timezone(timedelta(hours=5, minutes=30))
CACHE_FILE  = Path(__file__).parent / "backtest_1yr_cache.json"
OUTPUT_CSV  = Path(__file__).parent / "backtest_anand_trades.csv"

CSV_HEADERS = [
    "trade_id", "direction",
    "signal_bar_ist", "signal_bar_ts",
    "trigger_price",
    "entry_bar_ist",  "entry_bar_ts",
    "fill_price",
    "sl_price", "tp_price",
    "exit_bar_ist", "exit_bar_ts",
    "exit_price", "exit_type",
    "pts_raw", "pts_net_fee",
    "chop_avg_tr", "burst_threshold", "candle_body",
    "session",
]


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _ist(ts: int) -> str:
    return datetime.fromtimestamp(ts, IST).strftime("%d-%b-%Y %H:%M")

def _month_key(ts: int) -> str:
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m")

def _session_label(ts: int) -> str:
    h = datetime.fromtimestamp(ts, IST).hour
    if 11 <= h < 17: return "LON"
    if 18 <= h < 23: return "NY"
    if  6 <= h < 10: return "ASN"
    return "OFF"

def _fee(fill: float) -> float:
    """Round-trip taker fee in points (entry + exit)."""
    return round(fill * TAKER_FEE_PCT * 2, 4)


def load_cache(path: Path) -> list[Candle]:
    """Load JSON cache → list of Candle objects, sorted by ts ascending."""
    if not path.exists():
        print(f"[ERROR] Cache not found: {path}")
        print("  Run: python build_cache.py")
        sys.exit(1)
    raw = json.loads(path.read_text(encoding="utf-8"))
    candles = [
        Candle(ts=c["ts"], open=c["open"], high=c["high"],
               low=c["low"], close=c["close"], volume=c.get("volume", 0))
        for c in raw
    ]
    candles.sort(key=lambda c: c.ts)
    print(f"[CACHE] {len(candles):,} candles loaded  "
          f"({_ist(candles[0].ts)}  →  {_ist(candles[-1].ts)})")
    return candles


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY SIMULATION
# ════════════════════════════════════════════════════════════════════════════════

def simulate_entry(
    signal:        str,
    trigger_price: float,
    raw_candles:   list[Candle],
    sig_bar_idx:   int,
) -> Optional[tuple[int, float]]:
    """
    Simulate breakout entry on the 2 bars AFTER the signal bar.

    Pine logic (useLimitEntry=true, pLimit := high):
      BUY  — fill if next bar's open >= trigger (gap up)  → fill at open
             or   next bar's high >= trigger              → fill at trigger
      SELL — fill if next bar's open <= trigger (gap down) → fill at open
             or   next bar's low  <= trigger              → fill at trigger

    Returns (bar_index_of_fill, fill_price) or None if no fill within ENTRY_VALID_BARS.
    """
    total = len(raw_candles)
    for offset in range(1, ENTRY_VALID_BARS + 1):
        idx = sig_bar_idx + offset
        if idx >= total:
            break
        c = raw_candles[idx]
        if signal == "BUY":
            if c.open >= trigger_price:
                return (idx, round(c.open, 1))        # gap above — fill at open
            if c.high >= trigger_price:
                return (idx, round(trigger_price, 1)) # crossed trigger
        else:  # SELL
            if c.open <= trigger_price:
                return (idx, round(c.open, 1))
            if c.low <= trigger_price:
                return (idx, round(trigger_price, 1))
    return None


# ════════════════════════════════════════════════════════════════════════════════
# EXIT SIMULATION
# ════════════════════════════════════════════════════════════════════════════════

def simulate_exit(
    signal:     str,
    fill_price: float,
    raw_candles: list[Candle],
    fill_bar_idx: int,
) -> tuple[int, float, str]:
    """
    Simulate SL/TP exit on real OHLC candles (no HA — real prices).

    SL = fill ± FIXED_SL_PTS
    TP = fill ± FIXED_TP_PTS

    Intrabar logic (same bar can hit both):
      - If open gaps past TP → fill at open (favorable gap)
      - If open gaps past SL → fill at open (adverse gap)
      - Both TP and SL in same bar → conservative: SL wins
        EXCEPTION: if candle opens near entry and TP distance < SL distance
        from open, TP wins (price reached TP before retracing to SL).
        Approximation: TP wins if (open - fill) favors trade direction
        AND TP is closer to open than SL.

    Returns (bar_index, exit_price, exit_type).
    """
    d  = 1 if signal == "BUY" else -1
    sl = round(fill_price - d * FIXED_SL_PTS, 1)
    tp = round(fill_price + d * FIXED_TP_PTS, 1)

    total = len(raw_candles)
    for idx in range(fill_bar_idx, total):
        c = raw_candles[idx]

        if signal == "BUY":
            # Gap past TP
            if c.open >= tp:
                return (idx, round(c.open, 1), "TP_GAP")
            # Gap past SL
            if c.open <= sl:
                return (idx, round(c.open, 1), "SL_GAP")
            # Both TP and SL in same bar (pin bar / big range)
            tp_hit = c.high >= tp
            sl_hit = c.low  <= sl
            if tp_hit and sl_hit:
                # Conservative: SL wins unless open is much closer to TP side
                open_to_tp = abs(c.open - tp)
                open_to_sl = abs(c.open - sl)
                if open_to_tp < open_to_sl:
                    return (idx, tp, "TP")
                return (idx, sl, "SL")
            if tp_hit:
                return (idx, tp, "TP")
            if sl_hit:
                return (idx, sl, "SL")

        else:  # SELL
            if c.open <= tp:
                return (idx, round(c.open, 1), "TP_GAP")
            if c.open >= sl:
                return (idx, round(c.open, 1), "SL_GAP")
            tp_hit = c.low  <= tp
            sl_hit = c.high >= sl
            if tp_hit and sl_hit:
                open_to_tp = abs(c.open - tp)
                open_to_sl = abs(c.open - sl)
                if open_to_tp < open_to_sl:
                    return (idx, tp, "TP")
                return (idx, sl, "SL")
            if tp_hit:
                return (idx, tp, "TP")
            if sl_hit:
                return (idx, sl, "SL")

    # Should not happen — last candle always closes trade
    last = raw_candles[-1]
    return (total - 1, round(last.close, 1), "END_OF_DATA")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN BACKTEST LOOP
# ════════════════════════════════════════════════════════════════════════════════

def run_backtest(
    raw_candles:  list[Candle],
    backtest_days: int,
    use_fee:      bool,
) -> list[dict]:
    """
    Core backtest loop.

    Steps:
    1. Compute HA for all candles (batch, Pine-exact, single seed).
    2. Restrict backtest window to last `backtest_days` days.
    3. Walk bar-by-bar from WARMUP_BARS into the window:
       a. Build rolling window of HA candles (last BUFFER_SIZE bars).
       b. Call compute_indicators() with use_ha=False (HA already applied).
       c. On signal: compute trigger = HA candle's high (BUY) or low (SELL).
       d. Simulate breakout entry on next ENTRY_VALID_BARS real candles.
       e. Simulate SL/TP exit on real OHLC.
       f. Advance bar pointer past the exit bar to avoid overlapping trades.
    """
    n_total = len(raw_candles)

    # ── Step 1: Batch HA conversion ───────────────────────────────────────────
    print("[BACKTEST] Computing Heikin-Ashi for all candles...", end=" ", flush=True)
    ha_candles = compute_ha_candles(raw_candles)
    print(f"done ({len(ha_candles):,} bars)")

    # ── Step 2: Restrict backtest window ──────────────────────────────────────
    cutoff_ts = raw_candles[-1].ts - backtest_days * 86400
    bt_start  = next((i for i, c in enumerate(raw_candles) if c.ts >= cutoff_ts), 0)
    # Ensure warmup before backtest start
    bt_start  = max(WARMUP_BARS, bt_start)
    bt_end    = n_total

    start_ist = _ist(raw_candles[bt_start].ts)
    end_ist   = _ist(raw_candles[bt_end - 1].ts)
    print(f"[BACKTEST] Window: {start_ist}  →  {end_ist}  ({bt_end - bt_start:,} bars)")

    trades: list[dict] = []
    cooldown_left = 0
    trade_id      = 0
    i             = bt_start   # current bar pointer (advances past exit on each trade)

    while i < bt_end:
        # ── Build HA buffer for signal detection ──────────────────────────────
        buf_start = max(0, i - BUFFER_SIZE + 1)
        ha_buf    = deque(ha_candles[buf_start : i + 1], maxlen=BUFFER_SIZE)

        # ── Run signal detection ───────────────────────────────────────────────
        state = compute_indicators(ha_buf, SIG_CFG, cooldown_left, in_trade=False)

        if state is None:
            i += 1
            cooldown_left = max(0, cooldown_left - 1)
            continue

        # Decrement cooldown for this bar
        cooldown_left = max(0, cooldown_left - 1)

        if not state.signal:
            i += 1
            continue

        # ── Signal fired ──────────────────────────────────────────────────────
        sig      = state.signal                   # "BUY" or "SELL"
        sig_ts   = raw_candles[i].ts
        ha_bar   = ha_candles[i]

        # Trigger = HA candle's high (BUY) or low (SELL) — matches Pine exactly.
        # Pine: pLimit := high  (on HA chart → high = HA candle HIGH)
        # HA high = max(real_high, ha_open, ha_close) >= real_high
        # NOTE: live bot uses real candle high (_breakout_px = candle.high) — that
        #       may be a live bot bug. Backtest validates the Pine strategy, so HA high is correct.
        trigger  = ha_bar.high if sig == "BUY" else ha_bar.low

        # ── Entry simulation ──────────────────────────────────────────────────
        entry = simulate_entry(sig, trigger, raw_candles, i)

        if entry is None:
            # No breakout within ENTRY_VALID_BARS — skip, advance past valid window
            cooldown_left = SIG_CFG.cooldown
            i += ENTRY_VALID_BARS + 1
            continue

        fill_bar_idx, fill_price = entry

        # ── Exit simulation ───────────────────────────────────────────────────
        exit_bar_idx, exit_price, exit_type = simulate_exit(
            sig, fill_price, raw_candles, fill_bar_idx
        )

        # ── PnL ───────────────────────────────────────────────────────────────
        d       = 1 if sig == "BUY" else -1
        pts_raw = round((exit_price - fill_price) * d, 2)
        fee     = _fee(fill_price) if use_fee else 0.0
        pts_net = round(pts_raw - fee, 2)

        trade_id += 1
        trades.append({
            "trade_id":       trade_id,
            "direction":      sig,
            "signal_bar_ist": _ist(sig_ts),
            "signal_bar_ts":  sig_ts,
            "trigger_price":  trigger,
            "entry_bar_ist":  _ist(raw_candles[fill_bar_idx].ts),
            "entry_bar_ts":   raw_candles[fill_bar_idx].ts,
            "fill_price":     fill_price,
            "sl_price":       round(fill_price - d * FIXED_SL_PTS, 1),
            "tp_price":       round(fill_price + d * FIXED_TP_PTS, 1),
            "exit_bar_ist":   _ist(raw_candles[exit_bar_idx].ts),
            "exit_bar_ts":    raw_candles[exit_bar_idx].ts,
            "exit_price":     exit_price,
            "exit_type":      exit_type,
            "pts_raw":        pts_raw,
            "pts_net_fee":    pts_net,
            "chop_avg_tr":    round(state.chop_avg_tr, 1),
            "burst_threshold":round(state.burst_threshold, 1),
            "candle_body":    round(state.candle_body, 1),
            "session":        _session_label(sig_ts),
        })

        # Advance past exit bar, reset cooldown
        cooldown_left = SIG_CFG.cooldown
        i = exit_bar_idx + 1

    return trades


# ════════════════════════════════════════════════════════════════════════════════
# REPORTING
# ════════════════════════════════════════════════════════════════════════════════

def print_report(trades: list[dict], use_fee: bool):
    if not trades:
        print("\n[RESULT] No trades generated.")
        return

    pts_col   = "pts_net_fee" if use_fee else "pts_raw"
    wins      = [t for t in trades if t[pts_col] > 0]
    losses    = [t for t in trades if t[pts_col] <= 0]
    total_pts = round(sum(t[pts_col] for t in trades), 2)
    wr        = round(len(wins) / len(trades) * 100, 1)
    avg_pts   = round(total_pts / len(trades), 2)
    avg_win   = round(sum(t[pts_col] for t in wins)   / len(wins),   2) if wins   else 0
    avg_loss  = round(sum(t[pts_col] for t in losses) / len(losses), 2) if losses else 0

    # Max drawdown (peak-to-trough on cumulative pts)
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        cum  += t[pts_col]
        peak  = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    print("\n" + "═" * 60)
    print(f"  VOL SURGE v5 — ANAND BOT BACKTEST")
    print(f"  Config: SL={FIXED_SL_PTS}pts  TP={FIXED_TP_PTS}pts  "
          f"MinBody=50  Breakout={ENTRY_VALID_BARS}bars  HA=ON")
    print(f"  Fee: {'0.05% taker per side' if use_fee else 'DISABLED'}")
    print("═" * 60)
    print(f"  Total trades  : {len(trades)}")
    print(f"  Win Rate      : {wr}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Total pts     : {total_pts:+.1f}")
    print(f"  Avg pts/trade : {avg_pts:+.2f}")
    print(f"  Avg win       : {avg_win:+.2f} pts")
    print(f"  Avg loss      : {avg_loss:+.2f} pts")
    print(f"  Max drawdown  : {max_dd:.1f} pts")
    print(f"  Expectancy    : {avg_pts:+.2f} pts")
    print("═" * 60)

    # Monthly breakdown
    from collections import defaultdict
    monthly: dict[str, dict] = defaultdict(lambda: {"n": 0, "wins": 0, "pts": 0.0})
    for t in trades:
        mk = _month_key(t["signal_bar_ts"])
        monthly[mk]["n"]    += 1
        monthly[mk]["pts"]  += t[pts_col]
        if t[pts_col] > 0:
            monthly[mk]["wins"] += 1

    print(f"\n  {'Month':<12} {'Trades':>7} {'WR':>7} {'Pts':>10} {'Cumulative':>12}")
    print(f"  {'-'*12} {'-'*7} {'-'*7} {'-'*10} {'-'*12}")
    cum_pts = 0.0
    for month in sorted(monthly):
        m     = monthly[month]
        m_wr  = round(m["wins"] / m["n"] * 100, 0) if m["n"] else 0
        m_pts = round(m["pts"], 1)
        cum_pts += m_pts
        profit = "✅" if m_pts >= 0 else "🔴"
        print(f"  {month:<12} {m['n']:>7} {m_wr:>6.0f}% {m_pts:>+9.1f} {cum_pts:>+11.1f}  {profit}")

    # Direction breakdown
    buys  = [t for t in trades if t["direction"] == "BUY"]
    sells = [t for t in trades if t["direction"] == "SELL"]
    print(f"\n  Direction: BUY={len(buys)} ({round(len([t for t in buys if t[pts_col]>0])/len(buys)*100,1) if buys else 0}% WR)  "
          f"SELL={len(sells)} ({round(len([t for t in sells if t[pts_col]>0])/len(sells)*100,1) if sells else 0}% WR)")

    # Exit type breakdown
    from collections import Counter
    exit_counts = Counter(t["exit_type"] for t in trades)
    print(f"  Exit types: {dict(exit_counts)}")
    print("═" * 60)


def save_csv(trades: list[dict]):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(trades)
    print(f"\n[CSV] {len(trades)} trades saved → {OUTPUT_CSV.name}")


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Vol Surge v5 Backtest — Anand Bot")
    parser.add_argument("--days",    type=int,  default=365, help="Backtest window days (default: 365)")
    parser.add_argument("--no-fee",  action="store_true",    help="Disable commission (default: fee ON)")
    parser.add_argument("--no-csv",  action="store_true",    help="Skip CSV output")
    args = parser.parse_args()

    use_fee = not args.no_fee

    print("=" * 60)
    print("  Vol Surge v5 Backtest — Anand Bot")
    print(f"  SL={FIXED_SL_PTS}pts  TP={FIXED_TP_PTS}pts  Days={args.days}  Fee={'ON' if use_fee else 'OFF'}")
    print("=" * 60)

    # Load cache
    raw = load_cache(CACHE_FILE)

    # Run backtest
    trades = run_backtest(raw, backtest_days=args.days, use_fee=use_fee)

    # Report
    print_report(trades, use_fee)

    # Save CSV
    if not args.no_csv:
        save_csv(trades)


if __name__ == "__main__":
    main()
