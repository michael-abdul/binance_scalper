"""
strategy.py — Python Brain Layer
=================================
Replicates the MQL5 indicator logic exactly:
  • EMA (fast/slow) computed with numpy-free pure float deque
  • Spread filter  (InpMaxSpreadPts)
  • Tick-velocity filter (InpMinMovePts)
  • M1 trend gate (EMA cross state: bullish / bearish / cross)
  • Micro-pullback detection (replaces MT5 _GetSignal())
  • Risk gate: daily drawdown + consecutive losses
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional


# ── Signal enum mirrors ENUM_SIGNAL in MQL5 ──────────────────────
class Signal(IntEnum):
    HOLD =  0
    BUY  =  1
    SELL = -1


# ── EMA state — one instance per period ──────────────────────────
class EMA:
    """
    Incremental EMA (identical to iMA / MODE_EMA in MT5).
    alpha = 2 / (period + 1) — standard Wilder formula.
    """
    def __init__(self, period: int) -> None:
        self._alpha = 2.0 / (period + 1)
        self._value: Optional[float] = None

    def update(self, price: float) -> float:
        if self._value is None:
            self._value = price
        else:
            self._value = self._alpha * price + (1.0 - self._alpha) * self._value
        return self._value  # type: ignore[return-value]

    @property
    def value(self) -> Optional[float]:
        return self._value

    def warm(self) -> bool:
        return self._value is not None


# ── Tick velocity tracker ─────────────────────────────────────────
class TickVelocity:
    """
    Rolling n-tick average mid-price movement.
    Equivalent to MathAbs(bid - g_lastBid) in MT5, smoothed over a window.
    """
    def __init__(self, window: int = 1) -> None:
        self._moves: deque[float] = deque(maxlen=window)
        self._last_mid: Optional[float] = None

    def update(self, mid: float) -> float:
        if self._last_mid is not None:
            self._moves.append(abs(mid - self._last_mid))
        self._last_mid = mid
        if not self._moves:
            return 0.0
        return sum(self._moves) / len(self._moves)


# ── Spread EMA (mirrors _UpdateSpreadEma in StrategyModule.mqh) ──
class SpreadEMA:
    def __init__(self, period: int = 5) -> None:
        self._ema = EMA(period)

    def update(self, spread: float) -> float:
        return self._ema.update(spread)

    @property
    def value(self) -> float:
        return self._ema.value or 0.0


# ── Risk gate (mirrors CRiskEngine) ───────────────────────────────
@dataclass
class RiskState:
    max_consec_losses: int   = 6
    daily_dd_pct:      float = 3.0
    max_open_orders:   int   = 1
    consec_losses:     int   = field(default=0, init=False)
    day_start_balance: float = field(default=0.0, init=False)
    _last_day:         str   = field(default="", init=False)

    def reset_day(self, balance: float) -> None:
        today = time.strftime("%Y%m%d")
        if today != self._last_day:
            self.day_start_balance = balance
            self._last_day = today

    def is_approved(self, current_balance: float, open_count: int) -> tuple[bool, str]:
        if open_count >= self.max_open_orders:
            return False, f"open_positions={open_count} >= max={self.max_open_orders}"
        if self.consec_losses >= self.max_consec_losses:
            return False, f"consec_losses={self.consec_losses}"
        if self.day_start_balance > 0:
            dd = (self.day_start_balance - current_balance) / self.day_start_balance * 100
            if dd >= self.daily_dd_pct:
                return False, f"daily_drawdown={dd:.2f}% >= {self.daily_dd_pct}%"
        return True, "ok"

    def on_trade_close(self, is_win: bool) -> None:
        if is_win:
            self.consec_losses = 0
        else:
            self.consec_losses += 1


# ── Strategy parameters ───────────────────────────────────────────
@dataclass
class StrategyConfig:
    ema_fast_period: int   = 5
    ema_slow_period: int   = 20
    max_spread:      float = 0.60
    min_move:        float = 0.10
    velocity_window: int   = 1
    sl_dist:         float = 1.50
    tp_dist:         float = 1.00
    spread_sl_mult:  float = 1.5


# ── Entry result ──────────────────────────────────────────────────
@dataclass(frozen=True)
class EntryResult:
    signal:   Signal
    sl_price: float
    tp_price: float

    @staticmethod
    def hold() -> "EntryResult":
        return EntryResult(Signal.HOLD, 0.0, 0.0)


# ── Helpers (extracted to keep evaluate() complexity low) ─────────

def _compute_sl_tp(
    signal:     Signal,
    bid:        float,
    ask:        float,
    spread:     float,
    cfg:        StrategyConfig,
) -> tuple[float, float]:
    """Return (sl_price, tp_price) given direction and current quote."""
    sl_dist = cfg.sl_dist
    if spread > cfg.max_spread * 0.7:
        sl_dist *= cfg.spread_sl_mult

    if signal == Signal.BUY:
        return ask - sl_dist, ask + cfg.tp_dist
    return bid + sl_dist, bid - cfg.tp_dist


def _detect_pullback(
    new_trend: int,
    move:      float,
    min_move:  float,
) -> Signal:
    """
    Micro-pullback entry logic (replaces _GetSignal in MQL5).
    Bull trend + negative move → BUY the dip.
    Bear trend + positive move → SELL the rip.
    """
    if new_trend == 1 and move < -min_move:
        return Signal.BUY
    if new_trend == -1 and move > min_move:
        return Signal.SELL
    return Signal.HOLD


# ── Main strategy class ───────────────────────────────────────────
class TickStrategy:
    """
    Stateful strategy: call evaluate() on every tick from the Rust layer.
    Returns EntryResult(signal, sl_price, tp_price).
    """

    def __init__(self, cfg: StrategyConfig, risk: RiskState) -> None:
        self.cfg  = cfg
        self.risk = risk

        self._ema_fast   = EMA(cfg.ema_fast_period)
        self._ema_slow   = EMA(cfg.ema_slow_period)
        self._velocity   = TickVelocity(cfg.velocity_window)
        self._spread_ema = SpreadEMA(period=5)

        self._last_bid: Optional[float] = None
        self._trend: int = 0 

    # ── Warm-up check ─────────────────────────────────────────────
    def _indicators_ready(self) -> bool:
        return self._ema_fast.warm() and self._ema_slow.warm()

    # ── Trend direction ───────────────────────────────────────────
    def _update_trend(self, fast: float, slow: float) -> int:
        if fast > slow:
            return 1
        if fast < slow:
            return -1
        return 0

    # ── Risk check (collapsed into one call) ──────────────────────
    def _risk_ok(self, balance: float, open_count: int) -> bool:
        self.risk.reset_day(balance)
        approved, _ = self.risk.is_approved(balance, open_count)
        return approved

    # ── Main entry point ──────────────────────────────────────────
    def evaluate(
        self,
        bid:        float,
        ask:        float,
        balance:    float,
        open_count: int,
    ) -> EntryResult:
        """
        Returns EntryResult. sl/tp are 0.0 when signal == HOLD.

        NOTE: bid_qty / ask_qty are not used by this strategy version;
        they remain available via the tick object in main.py if needed
        for future order-book imbalance filters.
        """
        mid    = (bid + ask) * 0.5
        spread = ask - bid

        # ── Update all indicators first (always, for warm-up) ─────
        fast_val = self._ema_fast.update(mid)
        slow_val = self._ema_slow.update(mid)
        velocity = self._velocity.update(mid)
        self._spread_ema.update(spread)   # maintains internal EMA state

        # ── Gate 1: warm-up ───────────────────────────────────────
        if not self._indicators_ready():
            return EntryResult.hold()

        # ── Gate 2: spread filter ─────────────────────────────────
        if spread > self.cfg.max_spread:
            return EntryResult.hold()

        # ── Gate 3: risk ──────────────────────────────────────────
        if not self._risk_ok(balance, open_count):
            return EntryResult.hold()

        # ── Gate 4: tick velocity ─────────────────────────────────
        if velocity < self.cfg.min_move:
            return EntryResult.hold()

        # ── Gate 5: trend direction ───────────────────────────────
        new_trend  = self._update_trend(fast_val, slow_val)
        self._trend = new_trend
        if new_trend == 0:
            return EntryResult.hold()

        # ── Gate 6: micro-pullback ────────────────────────────────
        move       = 0.0 if self._last_bid is None else (bid - self._last_bid)
        self._last_bid = bid
        signal     = _detect_pullback(new_trend, move, self.cfg.min_move)

        if signal == Signal.HOLD:
            return EntryResult.hold()

        # ── Compute SL / TP ───────────────────────────────────────
        sl, tp = _compute_sl_tp(signal, bid, ask, spread, self.cfg)
        return EntryResult(signal, sl, tp)