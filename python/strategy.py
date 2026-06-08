"""
strategy.py — Python Brain Layer
=================================
Replicates the MQL5 indicator logic exactly:
  • EMA (fast/slow) computed with numpy-free pure float deque
    (avoids numpy import cost in tight loops)
  • Spread filter (raw points, identical to InpMaxSpreadPts)
  • Tick-velocity filter (min move, identical to InpMinMovePts)
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
    alpha = 2 / (period + 1)  — standard Wilder formula.
    """
    def __init__(self, period: int):
        self._alpha = 2.0 / (period + 1)
        self._value: Optional[float] = None

    def update(self, price: float) -> float:
        if self._value is None:
            self._value = price
        else:
            self._value = self._alpha * price + (1.0 - self._alpha) * self._value
        return self._value

    @property
    def value(self) -> Optional[float]:
        return self._value

    def warm(self) -> bool:
        return self._value is not None


# ── Tick velocity tracker ─────────────────────────────────────────
class TickVelocity:
    """
    Tracks the rolling n-tick average mid-price movement.
    Equivalent to `bidMove = MathAbs(bid - g_lastBid)` in MT5
    but smoothed over a window to reduce noise.
    """
    def __init__(self, window: int = 1):
        self._window = window
        self._moves: deque[float] = deque(maxlen=window)
        self._last_mid: Optional[float] = None

    def update(self, mid: float) -> float:
        if self._last_mid is not None:
            self._moves.append(abs(mid - self._last_mid))
        self._last_mid = mid
        if not self._moves:
            return 0.0
        return sum(self._moves) / len(self._moves)

    @property
    def last_move(self) -> float:
        return self._moves[-1] if self._moves else 0.0


# ── Spread EMA (mirrors _UpdateSpreadEma in StrategyModule.mqh) ──
class SpreadEMA:
    def __init__(self, period: int = 5):
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
    daily_dd_pct:      float = 3.0     # block when daily loss >= 3%
    max_open_orders:   int   = 1       # scalper: one position at a time
    consec_losses:     int   = field(default=0, init=False)
    day_start_balance: float = field(default=0.0, init=False)
    _last_day: str            = field(default="", init=False)

    def reset_day(self, balance: float):
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

    def on_trade_close(self, profit: float):
        if profit > 0:
            self.consec_losses = 0
        else:
            self.consec_losses += 1


# ── Strategy parameters ───────────────────────────────────────────
@dataclass
class StrategyConfig:
    # EMA periods (M1 timeframe equivalent)
    ema_fast_period:  int   = 5
    ema_slow_period:  int   = 20

    # Spread limits (in price units, same as InpMaxSpreadPts * point)
    max_spread:       float = 0.60   # e.g. 0.60 USDT for BTC/USDT

    # Minimum price movement to trigger entry (anti-noise)
    min_move:         float = 0.10   # in price units

    # Tick velocity: require at least N consecutive moves > min_move
    velocity_window:  int   = 1

    # SL / TP in price units (mirrors InpSLPoints * point)
    sl_dist:          float = 1.50
    tp_dist:          float = 1.00   # maker TP is tighter (lower fee cost)

    # Spread-adaptive entry: widen SL when spread is high
    spread_sl_mult:   float = 1.5    # SL *= this when spread > max_spread * 0.7


# ── Main strategy class ───────────────────────────────────────────
class TickStrategy:
    """
    Stateful strategy: call `evaluate(tick)` on every tick received
    from the Rust layer.  Returns (Signal, sl_price, tp_price) or
    (Signal.HOLD, 0.0, 0.0).
    """

    def __init__(self, cfg: StrategyConfig, risk: RiskState):
        self.cfg   = cfg
        self.risk  = risk

        self._ema_fast   = EMA(cfg.ema_fast_period)
        self._ema_slow   = EMA(cfg.ema_slow_period)
        self._velocity   = TickVelocity(cfg.velocity_window)
        self._spread_ema = SpreadEMA(period=5)

        self._last_bid: Optional[float] = None

        # Trend state: 1=bull, -1=bear, 0=cross/unknown
        self._trend: int = 0

    # ── Main entry point ──────────────────────────────────────────
    def evaluate(
        self,
        bid: float,
        ask: float,
        bid_qty: float,
        ask_qty: float,
        balance: float,
        open_count: int,
    ) -> tuple[Signal, float, float]:
        """
        Returns (Signal, sl_price, tp_price).
        sl/tp are 0.0 when Signal == HOLD.
        """
        mid    = (bid + ask) * 0.5
        spread = ask - bid

        # ── Update indicators ─────────────────────────────────────
        fast_val = self._ema_fast.update(mid)
        slow_val = self._ema_slow.update(mid)
        velocity = self._velocity.update(mid)
        spread_ema = self._spread_ema.update(spread)

        # Warm-up: need at least slow_period ticks before trading
        if not (self._ema_fast.warm() and self._ema_slow.warm()):
            return Signal.HOLD, 0.0, 0.0

        # ── 1. Spread filter (mirrors InpMaxSpreadPts check) ──────
        if spread > self.cfg.max_spread:
            return Signal.HOLD, 0.0, 0.0

        # ── 2. Risk gate ──────────────────────────────────────────
        self.risk.reset_day(balance)
        approved, reason = self.risk.is_approved(balance, open_count)
        if not approved:
            return Signal.HOLD, 0.0, 0.0

        # ── 3. Tick velocity / min-move filter ────────────────────
        if velocity < self.cfg.min_move:
            return Signal.HOLD, 0.0, 0.0

        # ── 4. Trend direction from EMA cross ─────────────────────
        # Mirrors _GetTrendDir() in TickScalperBot.mq5
        if fast_val > slow_val:
            new_trend = 1    # bullish
        elif fast_val < slow_val:
            new_trend = -1   # bearish
        else:
            new_trend = 0    # at-cross — wait

        cross = (new_trend != self._trend and self._trend != 0)
        self._trend = new_trend

        if new_trend == 0:
            return Signal.HOLD, 0.0, 0.0

        # ── 5. Micro-pullback entry (replaces _GetSignal) ─────────
        # Entry only on pullback INTO trend, not on new extensions.
        # Bull trend + bid just dipped (micro pullback) → BUY
        # Bear trend + ask just rose  (micro pullback) → SELL
        move = 0.0 if self._last_bid is None else (bid - self._last_bid)
        self._last_bid = bid

        signal = Signal.HOLD

        if new_trend == 1 and move < -self.cfg.min_move:
            # Pullback in uptrend — buy the dip
            signal = Signal.BUY

        elif new_trend == -1 and move > self.cfg.min_move:
            # Rally in downtrend — sell the rip
            signal = Signal.SELL

        if signal == Signal.HOLD:
            return Signal.HOLD, 0.0, 0.0

        # ── 6. SL / TP calculation ────────────────────────────────
        sl_dist = self.cfg.sl_dist
        # Widen SL when spread is elevated (spread-adaptive)
        if spread > self.cfg.max_spread * 0.7:
            sl_dist *= self.cfg.spread_sl_mult

        if signal == Signal.BUY:
            sl    = ask - sl_dist
            tp    = ask + self.cfg.tp_dist
        else:  # SELL
            sl    = bid + sl_dist
            tp    = bid - self.cfg.tp_dist

        return signal, sl, tp