"""
main.py — Binance Futures Tick Scalper Entry Point
====================================================
Architecture:
  Rust WebSocket  ──→  tick channel  ──→  Python strategy loop
                                               │
                                         Signal/SL/TP
                                               │
                                      Rust REST execution

Usage:
  export BINANCE_API_KEY="..."
  export BINANCE_SECRET="..."
  python main.py
"""
from dotenv import load_dotenv
load_dotenv()
import os
import sys
import time
import signal
import logging
from typing import Optional

# ── Local strategy brain ──────────────────────────────────────────
from strategy import (
    Signal, TickStrategy, StrategyConfig,
    RiskState,
)

# ── Rust compiled extension (built with: maturin develop) ─────────
try:
    import binance_scalper as bs          # compiled .so / .pyd
except ImportError:
    sys.exit(
        "ERROR: binance_scalper native module not found.\n"
        "Build it with:  maturin develop --release"
    )

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scalper")


# ── Configuration constants ───────────────────────────────────────
SYMBOL      = os.getenv("SCALPER_SYMBOL",  "BTCUSDT")
API_KEY     = os.getenv("BINANCE_API_KEY",  "")
API_SECRET  = os.getenv("BINANCE_SECRET",   "")

# Lot size in base asset (e.g. 0.001 BTC)
# Actual qty sent to Binance will be normalised to stepSize.
LOT_SIZE    = float(os.getenv("SCALPER_LOT", "0.001"))

# Polling interval when no ticks are buffered (seconds)
# Keep at 0 for minimal latency; increase to ~0.001 to reduce
# CPU usage on slower VPS at the cost of ~1 ms added latency.
POLL_SLEEP  = 0.0

# Maximum number of consecutive HOLD ticks before printing a
# heartbeat log (avoids silent-running confusion).
HEARTBEAT_TICKS = 5_000


def build_strategy() -> TickStrategy:
    """Construct strategy with params matching the MQL5 bot defaults."""
    cfg = StrategyConfig(
        ema_fast_period = 5,
        ema_slow_period = 20,
        max_spread      = 0.60,    # ~60 pts at BTC point=0.01
        min_move        = 0.10,
        velocity_window = 1,
        sl_dist         = 1.50,
        tp_dist         = 1.00,
        spread_sl_mult  = 1.5,
    )
    risk = RiskState(
        max_consec_losses = 6,
        daily_dd_pct      = 3.0,
        max_open_orders   = 1,
    )
    return TickStrategy(cfg, risk)


def main() -> None:
    if not API_KEY or not API_SECRET:
        sys.exit("ERROR: Set BINANCE_API_KEY and BINANCE_SECRET environment variables")

    log.info("Starting Binance Futures Tick Scalper — symbol=%s lot=%.4f", SYMBOL, LOT_SIZE)
    log.info("Rust engine version: %s", bs.__version__)

    # ── Initialise Rust engine ────────────────────────────────────
    engine = bs.ScalperEngine(SYMBOL, API_KEY, API_SECRET)
    engine.start()    # loads exchange rules, wallet, opens WebSocket

    log.info("Engine started. Waiting for ticks...")

    # ── Strategy brain ────────────────────────────────────────────
    strategy = build_strategy()

    # ── Live state ────────────────────────────────────────────────
    open_order_id: Optional[int] = None   # None when flat
    open_side:     Optional[str] = None
    open_tp:       float = 0.0
    open_sl:       float = 0.0

    tick_count   = 0
    order_count  = 0
    last_balance = 0.0

    # ── Graceful shutdown ─────────────────────────────────────────
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        log.info("Shutdown signal received — stopping loop")
        running = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Main tick loop ────────────────────────────────────────────
    while running:
        tick: Optional[bs.PyTick] = engine.poll_tick()

        if tick is None:
            # No tick available — yield briefly or spin
            if POLL_SLEEP > 0:
                time.sleep(POLL_SLEEP)
            continue

        tick_count += 1

        # ── Periodic balance refresh (every 200 ticks) ────────────
        if tick_count % 200 == 0:
            try:
                last_balance = engine.get_balance()
            except Exception as e:
                log.warning("Balance fetch failed: %s", e)

        # ── Heartbeat log ─────────────────────────────────────────
        if tick_count % HEARTBEAT_TICKS == 0:
            log.info(
                "Heartbeat | ticks=%d orders=%d balance=%.2f USDT | "
                "bid=%.5f ask=%.5f spread=%.5f",
                tick_count, order_count, last_balance,
                tick.bid, tick.ask, tick.spread,
            )

        # ── Position management: check SL/TP manually ─────────────
        # In a production system, rely on exchange-side SL/TP orders.
        # This Python-side check is a backup safety net.
        if open_order_id is not None:
            hit_tp = (open_side == "BUY"  and tick.bid >= open_tp) or \
                     (open_side == "SELL" and tick.ask <= open_tp)
            hit_sl = (open_side == "BUY"  and tick.bid <= open_sl) or \
                     (open_side == "SELL" and tick.ask >= open_sl)

            if hit_tp or hit_sl:
                close_side  = "SELL" if open_side == "BUY" else "BUY"
                close_price = tick.bid if close_side == "SELL" else tick.ask
                outcome     = "TP" if hit_tp else "SL"

                try:
                    engine.place_order(close_side, LOT_SIZE, close_price, reduce=True)
                    profit = (tick.bid - open_tp) if hit_tp else 0  # rough estimate
                    strategy.risk.on_trade_close(1.0 if hit_tp else -1.0)
                    log.info("[%s] Closed position | side=%s price=%.5f",
                             outcome, close_side, close_price)
                except Exception as e:
                    log.error("Close order failed: %s", e)

                open_order_id = None
                open_side     = None
            continue  # Don't evaluate new entries while in position

        # ── Strategy evaluation (flat only) ──────────────────────
        sig, sl, tp = strategy.evaluate(
            bid       = tick.bid,
            ask       = tick.ask,
            bid_qty   = tick.bid_qty,
            ask_qty   = tick.ask_qty,
            balance   = last_balance,
            open_count = 0 if open_order_id is None else 1,
        )

        if sig == Signal.HOLD:
            continue

        # ── Execute entry order ───────────────────────────────────
        side_str  = "BUY" if sig == Signal.BUY else "SELL"
        # For LIMIT POST_ONLY: queue at bid (buy) or ask (sell)
        # to act as maker and capture lower fee tier.
        limit_px  = tick.bid if sig == Signal.BUY else tick.ask

        try:
            result = engine.place_order(
                side   = side_str,
                qty    = LOT_SIZE,
                price  = limit_px,
                reduce = False,
            )
            open_order_id = result.order_id
            open_side     = side_str
            open_sl       = sl
            open_tp       = tp
            order_count  += 1

            log.info(
                "[ENTRY] %s | id=%d | limit=%.5f | SL=%.5f | TP=%.5f | "
                "spread=%.5f | balance=%.2f",
                side_str, result.order_id, limit_px, sl, tp,
                tick.spread, last_balance,
            )
        except Exception as e:
            log.error("Order placement failed: %s", e)

    # ── Shutdown summary ──────────────────────────────────────────
    log.info(
        "Stopped. Total ticks processed: %d | Total orders: %d",
        tick_count, order_count,
    )


if __name__ == "__main__":
    main()