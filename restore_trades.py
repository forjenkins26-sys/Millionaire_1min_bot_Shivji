import csv

HDR = ["trade_id","direction","mode","signal_timeframe","signal_tf_bar_time","pine_entry_px","fill_price","entry_slippage_pts","sl_price","tp_price","pine_signal_time","signal_recv_time","entry_fill_time","signal_latency_ms","entry_latency_ms","exit_price","exit_time","exit_type","exit_slippage_pts","pts","pnl_approx","python_actual_outcome","slippage_ratio","structure_grade","trade_duration_sec","monitor_cycles_total","recovery_event","recovery_reason","entry_order_id","api_request_time","api_ack_time","sl_placed_time","tp_placed_time","exit_order_id","exit_fill_px_delta","entry_slippage_pct","chop_avg_tr","burst_threshold","candle_body","atr5_prev","sl_dist_engine"]

ROWS = [
    ["B1780272240203","BUY","LIVE","1","1780272180","73771.25","73807.5","36.25","73757.5","73832.5","1780272240000","1780272240.2032604","1780272240.9620948","203.3","758.8","73832.5","2026-06-01T00:04:24.809427","TP_LIVE","0.0","25.0","0.025","TP","0.725","DEGRADED","23.8","50","False","","1339212891","1780272240.2084584","1780272240.9616084","1780272241.8694277","1780272241.8694277","1339212900","73832.5","0.0491","48.046","96.092","98.85","45.529","50.0"],
    ["B1780283100203","BUY","LIVE","1","1780283040","73936.0","73953.0","17.0","73903.0","73978.0","1780283100000","1780283100.2038224","1780283101.0106242","203.8","806.8","73899.5","2026-06-01T03:05:43.769146","SL_SOFTWARE","3.5","-53.5","-0.0535","SL","0.34","MILD","42.8","100","False","","1339353998","1780283100.2046187","1780283101.0102575","1780283101.891382","1780283101.891382","","73899.5","0.023","50.232","100.464","105.56","50.358","50.0"],
]

with open("/data/trades_v5.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(HDR)
    w.writerows(ROWS)

print(f"Restored {len(ROWS)} trades to /data/trades_v5.csv")
