# backtest/engine.py
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backtest.feed import MockTick, tick_stream
from strategy import Signal, TickStrategy, StrategyConfig, RiskState


# ── Lot map (production ile bir xil) ─────────────────────────
LOT_MAP = {
    "BTCUSDT":  0.001, "ETHUSDT":  0.02,  "BNBUSDT":  0.1,
    "SOLUSDT":  0.5,   "XRPUSDT":  50.0,  "ADAUSDT":  60.0,
    "DOGEUSDT": 200.0, "AVAXUSDT": 1.0,   "LINKUSDT": 2.0,
    "MATICUSDT":50.0,
}

DEFAULT_SPREAD_PCT   = 0.0008
DEFAULT_MIN_MOVE_PCT = 0.0002
DEFAULT_SL_PCT       = 0.0025
DEFAULT_TP_PCT       = 0.0015


def build_strategy(ref_price: float) -> TickStrategy:
    p = max(ref_price, 0.01)
    cfg = StrategyConfig(
        ema_fast_period = 5,
        ema_slow_period = 20,
        max_spread      = round(p * DEFAULT_SPREAD_PCT,   8),
        min_move        = round(p * DEFAULT_MIN_MOVE_PCT, 8),
        velocity_window = 1,
        sl_dist         = round(p * DEFAULT_SL_PCT,       8),
        tp_dist         = round(p * DEFAULT_TP_PCT,       8),
        spread_sl_mult  = 1.5,
    )
    risk = RiskState(max_consec_losses=6, daily_dd_pct=3.0, max_open_orders=1)
    return TickStrategy(cfg, risk)


# ── Trade log ─────────────────────────────────────────────────
@dataclass
class Trade:
    symbol:    str
    side:      str
    entry_px:  float
    exit_px:   float
    qty:       float
    pnl:       float
    outcome:   str   # "TP" | "SL" | "EXPIRE"
    entry_ts:  int
    exit_ts:   int


# ── Per-symbol sim state ──────────────────────────────────────
@dataclass
class SimState:
    symbol:    str
    lot:       float
    strategy:  Optional[TickStrategy] = None
    position:  Optional[dict]         = None   # {side, entry, sl, tp, ts}
    balance:   float                  = 10_000.0
    trades:    list[Trade]            = field(default_factory=list)

    def open_count(self) -> int:
        return 1 if self.position else 0


def _check_sl_tp(tick: MockTick, pos: dict) -> tuple[bool, bool]:
    if pos["side"] == "BUY":
        return tick.bid >= pos["tp"], tick.bid <= pos["sl"]
    return tick.ask <= pos["tp"], tick.ask >= pos["sl"]


def _calc_pnl(side: str, entry: float, exit_px: float, qty: float) -> float:
    if side == "BUY":
        return (exit_px - entry) * qty
    return (entry - exit_px) * qty


def _process(tick: MockTick, state: SimState) -> None:
    # Init strategy on first tick
    if state.strategy is None:
        state.strategy = build_strategy(tick.mid)

    # Open position: check SL/TP
    if state.position:
        pos = state.position
        hit_tp, hit_sl = _check_sl_tp(tick, pos)
        if hit_tp or hit_sl:
            exit_px = pos["tp"] if hit_tp else pos["sl"]
            pnl     = _calc_pnl(pos["side"], pos["entry"], exit_px, state.lot)
            state.balance += pnl
            state.trades.append(Trade(
                symbol   = state.symbol,
                side     = pos["side"],
                entry_px = pos["entry"],
                exit_px  = exit_px,
                qty      = state.lot,
                pnl      = pnl,
                outcome  = "TP" if hit_tp else "SL",
                entry_ts = pos["ts"],
                exit_ts  = tick.ts_ms,
            ))
            state.strategy.risk.on_trade_close(is_win=hit_tp)
            state.position = None
        return

    # Flat: evaluate strategy
    entry = state.strategy.evaluate(
        bid        = tick.bid,
        ask        = tick.ask,
        balance    = state.balance,
        open_count = state.open_count(),
    )
    if entry.signal == Signal.HOLD:
        return

    side     = "BUY" if entry.signal == Signal.BUY else "SELL"
    entry_px = tick.ask if side == "BUY" else tick.bid
    state.position = {
        "side":  side,
        "entry": entry_px,
        "sl":    entry.sl_price,
        "tp":    entry.tp_price,
        "ts":    tick.ts_ms,
    }


# ── Main runner ───────────────────────────────────────────────
def run_backtest(
    symbols:  list[str],
    data_dir: str = "data",
    interval: str = "1m",
) -> dict[str, list[Trade]]:

    states = {s: SimState(symbol=s, lot=LOT_MAP.get(s, 0.01)) for s in symbols}

    for symbol in symbols:
        path = Path(data_dir) / f"{symbol}_{interval}.csv"
        if not path.exists():
            print(f"[SKIP] {path} not found")
            continue

        print(f"[BT] Running {symbol}...")
        state = states[symbol]

        for tick in tick_stream(symbol, str(path)):
            _process(tick, state)

        # Close any open position at last price
        if state.position:
            state.position = None

        trades = state.trades
        if not trades:
            print(f"  → No trades")
            continue

        wins     = [t for t in trades if t.pnl > 0]
        total    = sum(t.pnl for t in trades)
        winrate  = len(wins) / len(trades) * 100
        print(f"  → trades={len(trades)} winrate={winrate:.1f}% "
              f"total_pnl={total:.2f} balance={state.balance:.2f}")

    return {s: states[s].trades for s in symbols}