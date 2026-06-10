# backtest/feed.py
from dataclasses import dataclass
from typing import Iterator
import pandas as pd


@dataclass
class MockTick:
    symbol:  str
    bid:     float
    ask:     float
    bid_qty: float
    ask_qty: float
    ts_ms:   int
    spread:  float
    mid:     float


SPREAD_PCT = 0.0002   # 0.02% simulated spread


def candle_to_ticks(symbol: str, row: pd.Series) -> list[MockTick]:
    """
    1 candle → 4 ticks: open, high, low, close
    Bullish candle:  open → low → high → close
    Bearish candle:  open → high → low → close
    """
    o, h, l, c = row["open"], row["high"], row["low"], row["close"]
    ts = int(row["open_time"])

    prices = [o, l, h, c] if c >= o else [o, h, l, c]

    ticks = []
    for i, price in enumerate(prices):
        spread = price * SPREAD_PCT
        half   = spread / 2
        ticks.append(MockTick(
            symbol  = symbol,
            bid     = price - half,
            ask     = price + half,
            bid_qty = 1.0,
            ask_qty = 1.0,
            ts_ms   = ts + i * 15_000,   # 15s apart
            spread  = spread,
            mid     = price,
        ))
    return ticks


def tick_stream(symbol: str, csv_path: str) -> Iterator[MockTick]:
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        yield from candle_to_ticks(symbol, row)