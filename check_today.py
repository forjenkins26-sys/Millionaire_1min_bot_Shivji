import sys, requests, time
from collections import deque
from datetime import datetime, timezone, timedelta
sys.path.insert(0, r"C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volSurge_1min_Anand_Live")
from candle_feed import Candle
from signal_engine import SignalConfig, SignalEngine
import logging; logging.disable(logging.CRITICAL)

IST = timezone(timedelta(hours=5, minutes=30))

# Fetch 300 1m candles from Delta (same as live bot)
end_ts   = int(time.time())
start_ts = end_ts - 700*60 - 120
r = requests.get("https://api.delta.exchange/v2/history/candles",
    params={"symbol":"BTCUSD","resolution":"1m","start":start_ts,"end":end_ts}, timeout=15)
candles_raw = r.json().get("result", [])
raw = sorted([Candle(ts=int(c["time"]), open=float(c["open"]), high=float(c["high"]),
              low=float(c["low"]), close=float(c["close"]), volume=float(c.get("volume",0)))
       for c in candles_raw], key=lambda x: x.ts)

cfg = SignalConfig(lookback=5, burst_mult=2.0, sl_mult=1.8, tp2_r=0.5,
                   cooldown=3, use_ha=False, use_ema_filter=False, use_session=False,
                   safety_factor=1.0, use_min_body=True, min_body_pts=50.0,
                   use_breakout_ctx=True, breakout_ctx_bars=5)
engine = SignalEngine(config=cfg)

ha_op=ha_cp=None; ha_buf=deque(maxlen=300)
targets = {"00:55","05:34","08:13","08:35"}

print(f"{'Time':<8} {'Signal':<7} {'HABody':>8} {'RawBody':>9} {'Chop':>7} {'Burst':>8} {'MinOK':>12} {'Brk':>5}")
print("-"*70)

for rc in raw:
    ha_c=(rc.open+rc.high+rc.low+rc.close)/4.0
    ha_o=(rc.open+rc.close)/2.0 if ha_op is None else (ha_op+ha_cp)/2.0
    ha_h=max(rc.high,ha_o,ha_c); ha_l=min(rc.low,ha_o,ha_c)
    hac=Candle(ts=rc.ts,open=round(ha_o,2),high=round(ha_h,2),low=round(ha_l,2),close=round(ha_c,2),volume=rc.volume)
    ha_buf.append(hac); ha_op,ha_cp=ha_o,ha_c
    if len(ha_buf)<10: continue

    ist=datetime.fromtimestamp(rc.ts,IST).strftime("%H:%M")
    if ist not in targets: continue

    st=engine.on_candle_close(hac,ha_buf,in_trade=False)
    raw_body=abs(rc.close-rc.open); ha_body=abs(ha_c-ha_o)
    if st:
        sig=st.signal or "—"
        min_ok=f"YES({ha_body:.0f}>=50)" if ha_body>=50 else f"NO({ha_body:.0f}<50)"
        recent_h=max(c.high for c in list(ha_buf)[-6:-1])
        recent_l=min(c.low  for c in list(ha_buf)[-6:-1])
        brk="Y" if (hac.close>recent_h or hac.close<recent_l) else "N"
        print(f"{ist:<8} {sig:<7} {ha_body:>8.1f} {raw_body:>9.1f} {st.chop_avg_tr:>7.1f} {st.burst_threshold:>8.1f} {min_ok:>12} {brk:>5}")
