#!/usr/bin/env python3
"""
backtest_correct.py — Vol Surge v5 Multi-TF Backtest (Pine-exact)
=================================================================
METHOD:
  - Incremental HA for signal detection (Pine-exact, use_ha=False)
  - REAL OHLC high/low for TP/SL exit checks (matches TV strategy fills)
  - REAL bar direction for TV OHLC intrabar assumption when both TP+SL hit same bar

USAGE:
  python backtest_correct.py                            # all TFs, full year summary
  python backtest_correct.py --tf 1m                   # 1m only, full year summary
  python backtest_correct.py --tf 1h --dates 05/2026   # 1H, May 2026 trade list
  python backtest_correct.py --tf 15m --dates 05/2026  # 15m, May 2026 trade list
  python backtest_correct.py --tf 1m --dates 24/05/2026  # 1m, specific date trade list

SUPPORTED TFs: 1m, 3m, 5m, 15m, 1h  (or 'all' for summary table)

Run from: C:\\Users\\ANAND SONI\\OneDrive\\Desktop\\TradingBots\\volSurge_1min\\
"""

import json
import sys
import logging
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

sys.path.insert(0, r"C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min")
from candle_feed import Candle
from signal_engine import SignalConfig, SignalEngine

logging.disable(logging.CRITICAL)

IST        = timezone(timedelta(hours=5, minutes=30))
CACHE_FILE = r"C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min\backtest_1yr_cache.json"

# ── Strategy config ───────────────────────────────────────────────────────────
FIXED_SL       = 150.0
FIXED_TP       = 200.0
BURST_MULT     = 2.0
MIN_BODY_PTS   = 50.0
CTX_BARS       = 5
LOOKBACK       = 5
COOLDOWN       = 3

TF_MINUTES = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "1h": 60}


# ── Resample 1m candles to higher TF ─────────────────────────────────────────
def resample(candles, tf_minutes):
    if tf_minutes == 1:
        return candles
    period  = tf_minutes * 60
    buckets = {}
    for c in candles:
        bt = (c.ts // period) * period
        if bt not in buckets:
            buckets[bt] = {"ts": bt, "open": c.open, "high": c.high,
                           "low": c.low, "close": c.close, "volume": c.volume}
        else:
            b = buckets[bt]
            b["high"]   = max(b["high"], c.high)
            b["low"]    = min(b["low"],  c.low)
            b["close"]  = c.close
            b["volume"] += c.volume
    return [
        Candle(ts=b["ts"], open=b["open"], high=b["high"],
               low=b["low"], close=b["close"], volume=b["volume"])
        for b in sorted(buckets.values(), key=lambda x: x["ts"])
    ]


# ── Core backtest ─────────────────────────────────────────────────────────────
def run_backtest(candles_all, date_filter=None):
    """
    date_filter examples:
      "05/2026"     → all trades in May 2026
      "24/05/2026"  → trades with entry on 24 May 2026
      "24/05"       → trades with that DD/MM (any year — avoid if cache spans 2 yrs)
    """
    cfg = SignalConfig(
        lookback          = LOOKBACK,
        burst_mult        = BURST_MULT,
        sl_mult           = 1.8,
        tp2_r             = FIXED_TP / FIXED_SL,
        cooldown          = COOLDOWN,
        use_ha            = False,
        use_ema_filter    = False,
        use_session       = False,
        safety_factor     = 1.0,
        use_min_body      = True,
        min_body_pts      = MIN_BODY_PTS,
        use_breakout_ctx  = True,
        breakout_ctx_bars = CTX_BARS,
    )
    engine   = SignalEngine(config=cfg)
    ha_op    = ha_cp = None
    in_trade = None
    trades   = []
    monthly  = defaultdict(float)
    ha_buf   = deque(maxlen=50)

    for rc in candles_all:
        # ── Incremental HA (signal detection only) ──────────────────────────
        ha_c_val = (rc.open + rc.high + rc.low + rc.close) / 4.0
        ha_o_val = (rc.open + rc.close) / 2.0 if ha_op is None else (ha_op + ha_cp) / 2.0
        ha_h_val = max(rc.high, ha_o_val, ha_c_val)
        ha_l_val = min(rc.low,  ha_o_val, ha_c_val)
        ha_c = Candle(ts=rc.ts,
                      open=round(ha_o_val, 2), high=round(ha_h_val, 2),
                      low=round(ha_l_val,  2), close=round(ha_c_val, 2),
                      volume=rc.volume)
        ha_buf.append(ha_c)
        ha_op, ha_cp = ha_o_val, ha_c_val

        if len(ha_buf) < 10:
            continue

        # ── Exit check (REAL OHLC, not HA) ──────────────────────────────────
        if in_trade:
            t, d = in_trade, in_trade["dir"]
            hit_tp = rc.high >= t["tp"] if d == "BUY" else rc.low  <= t["tp"]
            hit_sl = rc.low  <= t["sl"] if d == "BUY" else rc.high >= t["sl"]

            if hit_tp or hit_sl:
                if hit_tp and hit_sl:
                    # TV OHLC intrabar rule on REAL bar direction:
                    #   Bullish real bar (C>O): path open->low->high->close
                    #     BUY  -> low (SL) before high (TP) -> SL
                    #     SELL -> low (TP) before high (SL) -> TP
                    #   Bearish real bar (C<=O): path open->high->low->close
                    #     BUY  -> high (TP) before low (SL) -> TP
                    #     SELL -> high (SL) before low (TP) -> SL
                    rb = rc.close > rc.open
                    result = ("SL" if rb else "TP") if d == "BUY" else ("TP" if rb else "SL")
                else:
                    result = "SL" if hit_sl else "TP"

                pts      = FIXED_TP if result == "TP" else -FIXED_SL
                entry_dt = datetime.fromtimestamp(t["entry_ts"], IST)
                exit_dt  = datetime.fromtimestamp(rc.ts, IST)
                mk       = entry_dt.strftime("%Y-%m")
                monthly[mk] += pts
                trades.append(dict(
                    dir       = d,
                    entry_ist = entry_dt.strftime("%d/%m/%Y %H:%M"),
                    exit_ist  = exit_dt.strftime("%d/%m/%Y %H:%M"),
                    entry_px  = round(t["entry_px"], 1),
                    sl        = round(t["sl"],        1),
                    tp        = round(t["tp"],        1),
                    pts       = pts,
                    result    = result,
                    month     = mk,
                ))
                in_trade = None
            else:
                engine.on_candle_close(ha_c, ha_buf, in_trade=True)
                continue

        # ── Signal detection ─────────────────────────────────────────────────
        state = engine.on_candle_close(ha_c, ha_buf, in_trade=False)
        if state and state.signal in ("BUY", "SELL"):
            ep = state.close
            in_trade = dict(
                dir      = state.signal,
                entry_ts = rc.ts,
                entry_px = ep,
                sl       = ep - FIXED_SL if state.signal == "BUY" else ep + FIXED_SL,
                tp       = ep + FIXED_TP if state.signal == "BUY" else ep - FIXED_TP,
            )

    # ── Filter by date ────────────────────────────────────────────────────────
    if date_filter:
        trades = [t for t in trades if date_filter in t["entry_ist"]]

    return trades, dict(monthly)


# ── Print helpers ─────────────────────────────────────────────────────────────
def print_summary(tf_name, trades, monthly):
    n         = len(trades)
    wins      = sum(1 for t in trades if t["result"] == "TP")
    total     = sum(t["pts"] for t in trades)
    losing_mo = sum(1 for v in monthly.values() if v < 0)
    wr        = wins / n * 100 if n else 0
    exp       = total / n if n else 0

    print(f"\n{'='*70}")
    print(f"  {tf_name} | SL={FIXED_SL:.0f} TP={FIXED_TP:.0f} | BurstMult={BURST_MULT} MinBody={MIN_BODY_PTS:.0f} CtxBars={CTX_BARS}")
    print(f"{'='*70}")
    print(f"  Trades/yr  : {n:,}")
    print(f"  Win Rate   : {wr:.1f}%  ({wins}W  {n-wins}L)")
    print(f"  Total Pts  : {total:+,.0f}")
    print(f"  Expectancy : {exp:+.1f} pts/trade")
    print(f"  Losing Mo  : {losing_mo}/{len(monthly)}")
    if monthly:
        print(f"\n  Monthly breakdown:")
        for m, v in sorted(monthly.items()):
            flag = "" if v >= 0 else " <-- LOSS"
            print(f"    {m}: {v:+,.0f} pts{flag}")


def print_trade_list(tf_name, trades, date_filter):
    n     = len(trades)
    wins  = sum(1 for t in trades if t["result"] == "TP")
    total = sum(t["pts"] for t in trades)
    print(f"\n  {tf_name} | Filter: {date_filter} | {n} trades")
    print(f"\n  {'Dir':<5} {'Entry':<18} {'Exit':<18} {'EntryPx':<10} {'SL':<10} {'TP':<10} {'Pts':<7} Res")
    print("  " + "-"*84)
    for t in trades:
        print(f"  {t['dir']:<5} {t['entry_ist']:<18} {t['exit_ist']:<18} "
              f"{t['entry_px']:<10} {t['sl']:<10} {t['tp']:<10} {t['pts']:<7.0f} {t['result']}")
    print(f"\n  Total: {n} trades | {wins}W {n-wins}L | {total:+,.0f} pts")


def print_all_tf_table(candles_1m):
    print(f"\n{'All-TF Summary':^70}")
    print(f"{'='*70}")
    print(f"  {'TF':<6} {'Trades':>7} {'WR%':>7} {'Total Pts':>12} {'Expect':>9} {'LoseMo':>8}")
    print(f"  {'-'*52}")
    for tf_name, tf_min in TF_MINUTES.items():
        candles = resample(candles_1m, tf_min)
        trades, monthly = run_backtest(candles)
        n = len(trades)
        if n == 0:
            print(f"  {tf_name:<6} {'0':>7} {'-':>7} {'-':>12} {'-':>9} {'-':>8}")
            continue
        wins      = sum(1 for t in trades if t["result"] == "TP")
        total     = sum(t["pts"] for t in trades)
        losing_mo = sum(1 for v in monthly.values() if v < 0)
        print(f"  {tf_name:<6} {n:>7,} {wins/n*100:>6.1f}% {total:>+12,.0f} {total/n:>+8.1f} {losing_mo:>5}/{len(monthly)}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tf_arg      = None
    date_filter = None

    if "--tf" in sys.argv:
        tf_arg = sys.argv[sys.argv.index("--tf") + 1].lower()
    if "--dates" in sys.argv:
        date_filter = sys.argv[sys.argv.index("--dates") + 1]

    print(f"Loading cache...")
    with open(CACHE_FILE) as f:
        raw = json.load(f)
    candles_1m = [
        Candle(ts=c["ts"], open=c["open"], high=c["high"],
               low=c["low"], close=c["close"], volume=c["volume"])
        for c in raw
    ]
    first = datetime.fromtimestamp(candles_1m[0].ts,  IST).strftime("%d-%b-%Y")
    last  = datetime.fromtimestamp(candles_1m[-1].ts, IST).strftime("%d-%b-%Y")
    print(f"Loaded {len(candles_1m):,} candles  ({first} to {last})")

    # No TF specified or 'all' → summary table across all TFs
    if tf_arg is None or tf_arg == "all":
        print_all_tf_table(candles_1m)
        sys.exit(0)

    # Validate TF
    if tf_arg not in TF_MINUTES:
        print(f"ERROR: unknown TF '{tf_arg}'. Choose from: {', '.join(TF_MINUTES)}")
        sys.exit(1)

    candles = resample(candles_1m, TF_MINUTES[tf_arg])
    print(f"Resampled to {tf_arg}: {len(candles):,} candles")

    trades, monthly = run_backtest(candles, date_filter)

    if date_filter:
        print_trade_list(tf_arg.upper(), trades, date_filter)
    else:
        print_summary(tf_arg.upper(), trades, monthly)
