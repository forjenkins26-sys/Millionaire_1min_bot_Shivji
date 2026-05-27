#!/usr/bin/env python3
"""
build_cache.py — Download 1-year of 1m BTCUSD candles from Delta Exchange India
=================================================================================
Saves to: backtest_1yr_cache.json  (same folder as this script)

Usage:
    python build_cache.py              # last 365 days ending NOW
    python build_cache.py --days 400   # last 400 days

Pagination: 1-day chunks (1440 candles each) with 0.3s delay between requests.
Deduplication: timestamp-keyed dict — safe to re-run to extend/refresh cache.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

REST_URL   = "https://api.india.delta.exchange"
SYMBOL     = "BTCUSD"
RESOLUTION = "1m"
CHUNK_SECS = 86400        # 1 day per request (1440 candles — well within Delta limits)
DELAY      = 0.35         # seconds between requests (avoid rate limit)
CACHE_FILE = Path(__file__).parent / "backtest_1yr_cache.json"
IST        = timezone(timedelta(hours=5, minutes=30))


def fetch_chunk(start: int, end: int, session: requests.Session) -> list:
    """Fetch 1m candles for [start, end] epoch seconds. Returns list of raw dicts."""
    try:
        resp = session.get(
            f"{REST_URL}/v2/history/candles",
            params={"symbol": SYMBOL, "resolution": RESOLUTION,
                    "start": start, "end": end},
            timeout=20,
        )
        data = resp.json()
        candles = data.get("result", data.get("data", []))
        if not isinstance(candles, list):
            print(f"  [WARN] Unexpected response: {str(data)[:200]}")
            return []
        return candles
    except Exception as e:
        print(f"  [ERROR] fetch_chunk failed: {e}")
        return []


def parse_candle(raw: dict) -> dict | None:
    """Normalise one raw Delta candle to cache format."""
    try:
        ts = int(raw.get("time", raw.get("start", raw.get("candle_start_time", 0))))
        if ts == 0:
            return None
        if ts > 1_000_000_000_000_000:
            ts = ts // 1_000_000
        elif ts > 1_000_000_000_000:
            ts = ts // 1000
        return {
            "ts":     ts,
            "open":   float(raw.get("open",   0)),
            "high":   float(raw.get("high",   0)),
            "low":    float(raw.get("low",    0)),
            "close":  float(raw.get("close",  0)),
            "volume": float(raw.get("volume", raw.get("turnover", 0))),
        }
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=366,
                        help="Number of days of history to fetch (default: 366)")
    args = parser.parse_args()

    now       = int(time.time())
    end_ts    = now - 60          # exclude currently-forming bar
    start_ts  = end_ts - args.days * 86400

    start_ist = datetime.fromtimestamp(start_ts, IST).strftime("%d-%b-%Y %H:%M IST")
    end_ist   = datetime.fromtimestamp(end_ts,   IST).strftime("%d-%b-%Y %H:%M IST")
    print(f"Fetching {args.days} days of 1m BTCUSD candles")
    print(f"Range : {start_ist}  to  {end_ist}")
    print(f"Output: {CACHE_FILE}")
    print()

    # Load existing cache to avoid re-downloading already-held candles
    existing: dict[int, dict] = {}
    if CACHE_FILE.exists():
        try:
            raw_cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            for c in raw_cache:
                existing[c["ts"]] = c
            oldest = min(existing)
            newest = max(existing)
            print(
                f"Existing cache: {len(existing):,} candles  "
                f"({datetime.fromtimestamp(oldest, IST).strftime('%d-%b-%Y')} to "
                f"{datetime.fromtimestamp(newest, IST).strftime('%d-%b-%Y')})"
            )
            # Only fetch the missing portion (extend forward + fill start gap if needed)
            if oldest <= start_ts + 120:
                # existing cache covers the start; only top-up the end
                start_ts = newest + 60
                print(f"Cache up to {datetime.fromtimestamp(newest, IST).strftime('%d-%b-%Y %H:%M IST')} -- "
                      f"fetching only new candles from there")
        except Exception as e:
            print(f"[WARN] Could not load existing cache ({e}) — fetching full range")

    # Build list of chunk boundaries
    chunks = []
    chunk_start = start_ts
    while chunk_start < end_ts:
        chunk_end = min(chunk_start + CHUNK_SECS, end_ts)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    total_chunks = len(chunks)
    print(f"Chunks: {total_chunks}  ({CHUNK_SECS // 3600}h each, ~{CHUNK_SECS // 60} candles/chunk)\n")

    session   = requests.Session()
    new_count = 0
    now_epoch = int(time.time())

    for i, (cs, ce) in enumerate(chunks, 1):
        date_str = datetime.fromtimestamp(cs, IST).strftime("%d-%b-%Y")
        raw_list = fetch_chunk(cs, ce, session)

        chunk_new = 0
        for raw in raw_list:
            c = parse_candle(raw)
            if c and c["ts"] not in existing and c["ts"] + 60 <= now_epoch:
                existing[c["ts"]] = c
                chunk_new += 1

        new_count += chunk_new

        # Progress line (overwrite in terminal)
        pct = i / total_chunks * 100
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(
            f"\r  [{bar}] {pct:5.1f}%  chunk {i:>4}/{total_chunks}  "
            f"{date_str}  +{chunk_new:>4} new  total={len(existing):,}",
            end="", flush=True,
        )

        if i < total_chunks:
            time.sleep(DELAY)

    print()  # newline after progress bar
    print()

    if new_count == 0:
        print("No new candles fetched — cache already up to date.")
        sys.exit(0)

    # Sort by timestamp ascending and save
    sorted_candles = [existing[ts] for ts in sorted(existing)]

    first_ts = sorted_candles[0]["ts"]
    last_ts  = sorted_candles[-1]["ts"]
    first_ist = datetime.fromtimestamp(first_ts, IST).strftime("%d-%b-%Y %H:%M IST")
    last_ist  = datetime.fromtimestamp(last_ts,  IST).strftime("%d-%b-%Y %H:%M IST")

    CACHE_FILE.write_text(
        json.dumps(sorted_candles, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"[OK] Cache saved: {len(sorted_candles):,} candles")
    print(f"    Range : {first_ist}  to  {last_ist}")
    print(f"    New   : +{new_count:,} candles added")
    print(f"    File  : {CACHE_FILE}")


if __name__ == "__main__":
    main()
