import json, sys
from collections import deque
from datetime import datetime, timezone, timedelta
sys.path.insert(0, r"C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min_Anand_Live")
from candle_feed import Candle
from signal_engine import SignalConfig, SignalEngine
import logging; logging.disable(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))
CACHE = r"C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min_Anand_Live\backtest_1yr_cache.json"

with open(CACHE) as f:
    raw = [Candle(**c) for c in json.load(f)]

cfg = SignalConfig(lookback=5, burst_mult=2.0, sl_mult=1.8, tp2_r=0.5,
                   cooldown=3, use_ha=False, use_ema_filter=False, use_session=False,
                   safety_factor=1.0, use_min_body=True, min_body_pts=50.0,
                   use_breakout_ctx=True, breakout_ctx_bars=5)
engine = SignalEngine(config=cfg)

ha_op=ha_cp=None
ha_buf=deque(maxlen=300)

# Target times IST
targets = ["00:55", "05:34", "08:35", "08:13"]

print(f"{'Time':<8} {'Signal':<7} {'HABody':>8} {'ChopTR':>8} {'Burst':>8} {'MinOK':>6} {'BrkCtx':>7} {'RawBody':>8}")
print("-"*70)

for rc in raw:
    ha_c=(rc.open+rc.high+rc.low+rc.close)/4.0
    ha_o=(rc.open+rc.close)/2.0 if ha_op is None else (ha_op+ha_cp)/2.0
    ha_h=max(rc.high,ha_o,ha_c); ha_l=min(rc.low,ha_o,ha_c)
    hac=Candle(ts=rc.ts,open=round(ha_o,2),high=round(ha_h,2),low=round(ha_l,2),close=round(ha_c,2),volume=rc.volume)
    ha_buf.append(hac); ha_op,ha_cp=ha_o,ha_c
    if len(ha_buf)<10: continue

    ist=datetime.fromtimestamp(rc.ts,IST).strftime("%H:%M")
    # only print today (01/06) target times
    date_str=datetime.fromtimestamp(rc.ts,IST).strftime("%d/%m")
    if date_str != "01/06": continue
    if ist not in targets: continue

    st=engine.on_candle_close(hac, ha_buf, in_trade=False)
    raw_body=abs(rc.close-rc.open)
    ha_body=abs(ha_c-ha_o)
    if st:
        sig=st.signal or "—"
        chop=st.chop_avg_tr; burst=st.burst_threshold
        min_ok="YES" if ha_body>=50 else "NO"
        # breakout context check
        recent_h=max(c.high for c in list(ha_buf)[-6:-1])
        recent_l=min(c.low  for c in list(ha_buf)[-6:-1])
        brk="YES" if (hac.close>recent_h or hac.close<recent_l) else "NO"
        print(f"{ist:<8} {sig:<7} {ha_body:>8.1f} {chop:>8.1f} {burst:>8.1f} {min_ok:>6} {brk:>7} {raw_body:>8.1f}")
    else:
        print(f"{ist:<8} {'—':<7} {ha_body:>8.1f} {'?':>8} {'?':>8} {'?':>6} {'?':>7} {raw_body:>8.1f}")
