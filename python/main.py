"""
main.py — Binance Futures Tick Scalper (Multi-Symbol)
======================================================
Architecture:
  ONE shared ScalperEngine (one WS connection, one rate limiter)
    Rust WebSocket  ──→  tick channel  ──→  Python strategy loop
                                                 │
                                           EntryResult (signal/sl/tp)
                                                 │
                              LIMIT(entry) / MARKET(close) REST

Multi-symbol via threads — each symbol runs an independent
TickStrategy in its own daemon thread, routing ticks by symbol.

Usage:
  SCALPER_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT python main.py
  SCALPER_SYMBOL=BTCUSDT python main.py          # single (backwards compat)
"""
from dotenv import load_dotenv
load_dotenv()

import os
import sys
import time
import signal
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

from strategy import Signal, TickStrategy, StrategyConfig, RiskState, EntryResult

try:
    import binance_scalper as bs
except ImportError:
    sys.exit(
        "ERROR: binance_scalper native module not found.\n"
        "Build it with:  maturin develop --release"
    )

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Global config ─────────────────────────────────────────────
API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_SECRET", "")

# Multi-symbol: comma-separated, e.g. "BTCUSDT,ETHUSDT,SOLUSDT"
_sym_env = os.getenv("SCALPER_SYMBOLS") or os.getenv("SCALPER_SYMBOL", "BTCUSDT")
SYMBOLS  = [s.strip().upper() for s in _sym_env.split(",") if s.strip()]

# Per-symbol lot sizes: "BTCUSDT=0.001,ETHUSDT=0.01" or single SCALPER_LOT
_lot_default = float(os.getenv("SCALPER_LOT", "0.001"))
_lot_env     = os.getenv("SCALPER_LOTS", "")
LOT_MAP: dict[str, float] = {}
if _lot_env:
    for part in _lot_env.split(","):
        if "=" in part:
            sym, val = part.split("=", 1)
            LOT_MAP[sym.strip().upper()] = float(val.strip())

def get_lot(symbol: str) -> float:
    return LOT_MAP.get(symbol, _lot_default)

POLL_SLEEP            = float(os.getenv("SCALPER_POLL_SLEEP",      "0.0"))
HEARTBEAT_TICKS       = int(os.getenv("SCALPER_HEARTBEAT",         "5000"))
PENDING_POLL_EVERY    = int(os.getenv("SCALPER_PENDING_POLL",       "3"))
BALANCE_REFRESH_EVERY = int(os.getenv("SCALPER_BALANCE_REFRESH",    "200"))
CLOSE_RETRY_COOLDOWN  = float(os.getenv("SCALPER_CLOSE_COOLDOWN",  "0.4"))
MAX_CLOSE_RETRIES     = int(os.getenv("SCALPER_MAX_CLOSE_RETRIES",  "3"))

# ── Shared engine (one WS + one rate limiter for all symbols) ─
_engine:      Optional["bs.ScalperEngine"] = None
_engine_lock: threading.Lock = threading.Lock()

def get_or_create_engine() -> "bs.ScalperEngine":
    global _engine
    with _engine_lock:
        if _engine is None:
            _engine = bs.ScalperEngine(SYMBOLS, API_KEY, API_SECRET)
            _engine.start(SYMBOLS)
        return _engine

# ── Position state ────────────────────────────────────────────

@dataclass
class PositionState:
    order_id:    int
    side:        str
    sl:          float
    tp:          float
    close_tries: int = 0


@dataclass
class PendingState:
    order_id: int
    side:     str
    sl:       float
    tp:       float


@dataclass
class SymbolState:
    symbol:                str
    lot_size:              float
    position:              Optional[PositionState] = None
    pending:               Optional[PendingState]  = None
    last_close_attempt_ts: float                   = field(default=0.0)
    tick_count:            int                     = field(default=0)
    order_count:           int                     = field(default=0)
    last_balance:          float                   = field(default=0.0)

    @property
    def is_flat(self) -> bool:
        return self.position is None and self.pending is None

    @property
    def open_count(self) -> int:
        return 0 if self.is_flat else 1


# ── Helpers ───────────────────────────────────────────────────

def is_binance_code(exc: Exception, code: str) -> bool:
    return code in str(exc)


def maker_safe_price(side: str, bid: float, ask: float, tick_size: float) -> float:
    return max(0.0, bid - tick_size) if side == "BUY" else ask + tick_size


# ── Dynamic thresholds — % of price, works for any coin ──────  # ← NEW
DEFAULT_SPREAD_PCT   = 0.0008   # 0.08% → BTC≈50, ETH≈1.3, SOL≈0.05
DEFAULT_MIN_MOVE_PCT = 0.0002   # 0.02%
DEFAULT_SL_PCT       = 0.0025   # 0.25%
DEFAULT_TP_PCT       = 0.0015   # 0.15%

def build_strategy(ref_price: float) -> TickStrategy:   # ← CHANGED
    p = max(ref_price, 0.01)
    cfg = StrategyConfig(
        ema_fast_period = 5,
        ema_slow_period = 20,
        max_spread      = round(p * DEFAULT_SPREAD_PCT,   8),         # ← CHANGED
        min_move        = round(p * DEFAULT_MIN_MOVE_PCT, 8),         # ← CHANGED
        velocity_window = 1,
        sl_dist         = round(p * DEFAULT_SL_PCT,       8),         # ← CHANGED
        tp_dist         = round(p * DEFAULT_TP_PCT,       8),         # ← CHANGED
        spread_sl_mult  = 1.5,
    )
    risk = RiskState(
        max_consec_losses=6,
        daily_dd_pct=3.0,
        max_open_orders=1,
    )
    return TickStrategy(cfg, risk)


# ── Sub-handlers ──────────────────────────────────────────────

def _refresh_balance(
    engine: "bs.ScalperEngine", state: SymbolState, log: logging.Logger
) -> None:
    try:
        state.last_balance = engine.get_balance()
    except Exception:
        log.exception("Balance fetch failed")


def _poll_pending(
    engine: "bs.ScalperEngine", state: SymbolState, log: logging.Logger
) -> None:
    assert state.pending is not None
    p = state.pending
    try:
        status, executed_qty, avg_price = engine.get_order_status(state.symbol, p.order_id)
    except Exception:
        log.exception("Pending status check failed id=%d", p.order_id)
        return

    if status in ("FILLED", "PARTIALLY_FILLED") and executed_qty > 0.0:
        state.position = PositionState(p.order_id, p.side, p.sl, p.tp)
        state.pending  = None
        log.info("[ENTRY-FILLED] id=%d status=%s qty=%.6f avg=%.5f",
                 p.order_id, status, executed_qty, avg_price)
    elif status in ("CANCELED", "EXPIRED", "REJECTED"):
        log.info("[ENTRY-DEAD] id=%d status=%s", p.order_id, status)
        state.pending = None


def _check_sl_tp(tick: "bs.PyTick", pos: PositionState) -> tuple[bool, bool]:
    if pos.side == "BUY":
        return tick.bid >= pos.tp, tick.bid <= pos.sl
    return tick.ask <= pos.tp, tick.ask >= pos.sl


def _close_with_market(
    engine:   "bs.ScalperEngine",
    state:    SymbolState,
    strategy: TickStrategy,
    tick:     "bs.PyTick",
    outcome:  str,
    log:      logging.Logger,
) -> None:
    """MARKET close — guaranteed fill. Never uses GTX/POST_ONLY."""
    assert state.position is not None
    pos        = state.position
    close_side = "SELL" if pos.side == "BUY" else "BUY"

    try:
        pos_size = engine.get_position_size(state.symbol)
    except Exception:
        log.exception("[%s] get_position_size failed", outcome)
        return

    if pos_size <= 0.0:
        log.warning("[%s] No live position on exchange — clearing state", outcome)
        state.position = None
        return

    close_qty    = min(state.lot_size, pos_size)
    pos.close_tries += 1

    try:
        engine.place_market_order(state.symbol, close_side, close_qty, reduce=True)
    except Exception:
        log.exception("[%s] MARKET close failed (attempt %d/%d)",
                      outcome, pos.close_tries, MAX_CLOSE_RETRIES)
        if pos.close_tries >= MAX_CLOSE_RETRIES:
            log.error("[%s] Max retries hit — clearing state. "
                      "CHECK EXCHANGE MANUALLY!", outcome)
            state.position = None
        return

    strategy.risk.on_trade_close(is_win=(outcome == "TP"))
    log.info("[%s] MARKET close sent | side=%s qty=%.6f | bid=%.5f ask=%.5f",
             outcome, close_side, close_qty, tick.bid, tick.ask)
    state.position = None


def _manage_open_position(
    engine:   "bs.ScalperEngine",
    state:    SymbolState,
    strategy: TickStrategy,
    tick:     "bs.PyTick",
    log:      logging.Logger,
) -> None:
    assert state.position is not None
    hit_tp, hit_sl = _check_sl_tp(tick, state.position)
    if not (hit_tp or hit_sl):
        return
    now = time.time()
    if now - state.last_close_attempt_ts < CLOSE_RETRY_COOLDOWN:
        return
    state.last_close_attempt_ts = now
    _close_with_market(engine, state, strategy, tick,
                       "TP" if hit_tp else "SL", log)


def _register_entry_result(
    state:    SymbolState,
    result:   "bs.PyOrderResult",
    side_str: str,
    limit_px: float,
    entry:    EntryResult,
    log:      logging.Logger,
) -> None:
    if result.status in ("FILLED", "PARTIALLY_FILLED") and result.executed_qty > 0.0:
        state.position = PositionState(result.order_id, side_str,
                                       entry.sl_price, entry.tp_price)
        log.info("[ENTRY-FILLED] %s | id=%d | limit=%.5f | avg=%.5f | SL=%.5f | TP=%.5f",
                 side_str, result.order_id, limit_px,
                 result.avg_price, entry.sl_price, entry.tp_price)
    else:
        state.pending = PendingState(result.order_id, side_str,
                                     entry.sl_price, entry.tp_price)
        log.info("[ENTRY-PENDING] %s | id=%d | limit=%.5f | SL=%.5f | TP=%.5f | status=%s",
                 side_str, result.order_id, limit_px,
                 entry.sl_price, entry.tp_price, result.status)


def _place_entry(
    engine:    "bs.ScalperEngine",
    state:     SymbolState,
    side_str:  str,
    limit_px:  float,
    entry:     EntryResult,
    tick_size: float,
    tick:      "bs.PyTick",
    log:       logging.Logger,
) -> None:
    try:
        result = engine.place_order(state.symbol, side_str, state.lot_size, limit_px, reduce=False)
    except Exception as exc:
        if is_binance_code(exc, '"code":-5022'):
            # Price crossed spread — retry one tick deeper as maker
            retry_px = maker_safe_price(side_str, tick.bid, tick.ask, tick_size)
            try:
                result = engine.place_order(state.symbol, side_str, state.lot_size, retry_px, reduce=False)
            except Exception:
                log.exception("LIMIT retry failed after -5022")
                return
            state.order_count += 1
            _register_entry_result(state, result, side_str, retry_px, entry, log)
            log.info("[ENTRY-RETRY] %s retry_px=%.5f status=%s",
                     side_str, retry_px, result.status)
        else:
            log.exception("Order placement failed")
        return

    state.order_count += 1
    _register_entry_result(state, result, side_str, limit_px, entry, log)


# ── Tick processor (DRY — used for first tick + main loop) ───  # ← NEW
def _process_tick(
    tick:      "bs.PyTick",
    engine:    "bs.ScalperEngine",
    state:     SymbolState,
    strategy:  TickStrategy,
    tick_size: float,
    log:       logging.Logger,
) -> None:
    state.tick_count += 1

    if state.tick_count % BALANCE_REFRESH_EVERY == 0:
        _refresh_balance(engine, state, log)

    if state.tick_count % HEARTBEAT_TICKS == 0:
        log.info("Heartbeat | ticks=%d orders=%d balance=%.2f | "
                 "bid=%.5f ask=%.5f spread=%.5f",
                 state.tick_count, state.order_count, state.last_balance,
                 tick.bid, tick.ask, tick.spread)

    if state.pending is not None:
        if state.tick_count % PENDING_POLL_EVERY == 0:
            _poll_pending(engine, state, log)
        return

    if state.position is not None:
        _manage_open_position(engine, state, strategy, tick, log)
        return

    entry = strategy.evaluate(
        bid        = tick.bid,
        ask        = tick.ask,
        balance    = state.last_balance,
        open_count = state.open_count,
    )
    if entry.signal == Signal.HOLD:
        return

    side_str = "BUY" if entry.signal == Signal.BUY else "SELL"
    limit_px = tick.bid if entry.signal == Signal.BUY else tick.ask
    _place_entry(engine, state, side_str, limit_px, entry, tick_size, tick, log)


# ── Per-symbol loop ───────────────────────────────────────────
# ── NEW: extracted helpers ────────────────────────────────────

def _wait_for_first_tick(
    symbol:     str,
    engine:     "bs.ScalperEngine",
    stop_event: threading.Event,
) -> Optional["bs.PyTick"]:
    while not stop_event.is_set():
        t = engine.poll_tick()
        if t is not None and t.symbol == symbol:
            return t
        time.sleep(0.01)
    return None


def _run_tick_loop(
    symbol:     str,
    engine:     "bs.ScalperEngine",
    state:      SymbolState,
    strategy:   TickStrategy,
    tick_size:  float,
    stop_event: threading.Event,
    log:        logging.Logger,
) -> None:
    while not stop_event.is_set():
        tick: Optional[bs.PyTick] = engine.poll_tick()
        if tick is None:
            if POLL_SLEEP > 0:
                time.sleep(POLL_SLEEP)
            continue
        if tick.symbol != symbol:
            continue
        _process_tick(tick, engine, state, strategy, tick_size, log)


# ── Per-symbol loop ───────────────────────────────────────────

def run_symbol(symbol: str, stop_event: threading.Event) -> None:
    log = logging.getLogger(f"scalper.{symbol}")
    log.info("Starting | lot=%.4f", get_lot(symbol))

    try:
        engine = get_or_create_engine()
    except Exception:
        log.exception("Engine start failed — thread exiting")
        return

    precision = engine.get_precision()
    px_rule   = precision.get(symbol)
    tick_size = px_rule[2] if px_rule else 0.1

    first_tick = _wait_for_first_tick(symbol, engine, stop_event)
    if first_tick is None:
        return

    strategy = build_strategy(first_tick.mid)
    state    = SymbolState(symbol=symbol, lot_size=get_lot(symbol))

    log.info("Engine ready | tick_size=%.4f | ref_price=%.5f | "
             "max_spread=%.5f | min_move=%.5f | sl=%.5f | tp=%.5f | Rust v%s",
             tick_size, first_tick.mid,
             strategy.cfg.max_spread, strategy.cfg.min_move,
             strategy.cfg.sl_dist,    strategy.cfg.tp_dist,
             bs.__version__)

    _process_tick(first_tick, engine, state, strategy, tick_size, log)
    _run_tick_loop(symbol, engine, state, strategy, tick_size, stop_event, log)

    log.info("Stopped | ticks=%d orders=%d", state.tick_count, state.order_count)

# ── Main ──────────────────────────────────────────────────────

def main() -> None:
    if not API_KEY or not API_SECRET:
        sys.exit("ERROR: Set BINANCE_API_KEY and BINANCE_SECRET env vars")

    if not SYMBOLS:
        sys.exit("ERROR: Set SCALPER_SYMBOLS=BTCUSDT,ETHUSDT or SCALPER_SYMBOL=BTCUSDT")

    root_log = logging.getLogger("scalper")
    root_log.info("Launching %d symbol(s): %s", len(SYMBOLS), ", ".join(SYMBOLS))

    stop_event = threading.Event()

    def _shutdown(sig: int, _frame: object) -> None:
        root_log.info("Shutdown signal — stopping all threads")
        stop_event.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    threads: list[threading.Thread] = []
    for sym in SYMBOLS:
        t = threading.Thread(
            target=run_symbol,
            args=(sym, stop_event),
            name=f"scalper-{sym}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    root_log.info("All threads stopped.")


if __name__ == "__main__":
    main()