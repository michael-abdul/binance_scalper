# backtest/download.py
import time
import requests
import pandas as pd
from pathlib import Path

BASE = "https://fapi.binance.com"
OUT  = Path("data")
OUT.mkdir(exist_ok=True)

def fetch_klines(symbol: str, interval: str, days: int) -> pd.DataFrame:
    """
    interval: '1m' | '3m' | '5m'
    days: 365 = 1 yil
    """
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - days * 86_400_000
    rows = []

    while start_ms < end_ms:
        resp = requests.get(f"{BASE}/fapi/v1/klines", params={
            "symbol":    symbol,
            "interval":  interval,
            "startTime": start_ms,
            "limit":     1500,
        }, timeout=10)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        start_ms = batch[-1][0] + 1
        time.sleep(0.3)   # rate limit

    df = pd.DataFrame(rows, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","taker_buy_base",
        "taker_buy_quote","ignore"
    ])
    df["open_time"] = pd.to_numeric(df["open_time"])
    for col in ["open","high","low","close"]:
        df[col] = df[col].astype(float)
    return df

if __name__ == "__main__":
    SYMBOLS  = ["BTCUSDT", "ETHUSDT", "SOLUSDT",
                "BNBUSDT", "XRPUSDT", "ADAUSDT",
                "DOGEUSDT","AVAXUSDT","LINKUSDT","MATICUSDT"]
    INTERVAL = "1m"
    DAYS     = 365

    for sym in SYMBOLS:
        print(f"Downloading {sym}...")
        df = fetch_klines(sym, INTERVAL, DAYS)
        path = OUT / f"{sym}_{INTERVAL}.csv"
        df.to_csv(path, index=False)
        print(f"  → {len(df)} rows saved to {path}")