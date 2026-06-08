# Binance Futures Tick Scalper — Rust + Python (PyO3)

Migration of the MT5 MQL5 TickScalperBot to Binance USDⓈ-M Futures.  
Architecture: **Rust "Hands"** (WebSocket + REST) + **Python "Brain"** (strategy logic).

---

## Architecture

```
Binance fstream  ──WebSocket──►  Rust tokio task
                                      │  (tick channel mpsc)
                                      ▼
                              Python strategy loop
                              (EMA cross + velocity +
                               spread filter + risk gate)
                                      │
                                  Signal + SL/TP
                                      │
                              Rust REST client
                              (HMAC-SHA256 signed,
                               LIMIT POST_ONLY order)
                                      │
                              Binance Futures API
```

### Layer responsibilities

| Layer | File | Mirrors MT5 |
|-------|------|-------------|
| WebSocket ingest | `src/websocket.rs` | `OnTick()` event source |
| Rate limiter | `src/rate_limiter.rs` | — (not needed in MT5) |
| REST execution | `src/execution.rs` | `CExecutionWrapper` |
| PyO3 bindings | `src/lib.rs` | — |
| Strategy brain | `python/strategy.py` | `TickScalperBot.mq5` + `StrategyModule.mqh` |
| Risk gate | `python/strategy.py` RiskState | `CRiskEngine` |
| Entry point | `python/main.py` | `OnTick()` + `OnInit()` |

---

## Prerequisites

```bash
# Ubuntu 22.04 / 24.04 on VPS
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

pip install maturin
```

---

## Build

```bash
# Development (debug symbols, faster compile)
maturin develop

# Production (optimised, strip symbols)
maturin develop --release

# Build distributable wheel
maturin build --release
```

---

## Configuration

```bash
export BINANCE_API_KEY="your_api_key_here"
export BINANCE_SECRET="your_secret_here"
export SCALPER_SYMBOL="BTCUSDT"
export SCALPER_LOT="0.001"           # BTC per trade
```

---

## Run

```bash
cd python
python main.py
```

---

## Run Tests

```bash
# Rust unit tests
cargo test

# Python strategy tests
pip install pytest
pytest tests/test_strategy.py -v
```

---

## Key Design Decisions vs MT5

### Order type: LIMIT POST_ONLY (GTX)
MT5 uses market orders for instant fill. On Binance, **POST_ONLY (GTX)**  
guarantees maker-fee tier (≤0.02% vs 0.05% taker). The Python layer  
queues orders at the current bid/ask, which fills within 1–3 ticks on  
liquid pairs like BTC/USDT.

### Precision normalization
MT5 uses `_Digits` + `NormalizeDouble`. Binance requires `tickSize`  
and `stepSize` from `/fapi/v1/exchangeInfo` — loaded at startup and  
applied in `execution.rs::normalize_price()` / `normalize_qty()`.

### Auto-reconnect
`websocket.rs::run_stream_loop` uses exponential back-off (500ms → 60s)  
on any error. Ping frames are sent every 170s to prevent Binance's  
3-minute inactivity disconnect.

### Thread model
- 2 tokio worker threads: one for WS, one for REST.
- Python GIL is held only during `poll_tick()` / `place_order()`.
- Tick production (Rust) is fully GIL-free — zero Python overhead  
  on the hot data path.

---

## MQL5 → Binance Parameter Mapping

| MQL5 Parameter | Python Equivalent | Notes |
|---|---|---|
| `InpMaxSpreadPts` | `StrategyConfig.max_spread` | In price units (USDT), not points |
| `InpMinMovePts` | `StrategyConfig.min_move` | Same unit |
| `InpSLPoints` | `StrategyConfig.sl_dist` | price units |
| `InpTPPoints` | `StrategyConfig.tp_dist` | price units |
| `InpEmaFast` | `StrategyConfig.ema_fast_period` | identical |
| `InpEmaSlow` | `StrategyConfig.ema_slow_period` | identical |
| `InpMagic` | — | Not needed (single account) |
| `InpDisableRisk` | `RiskState` fields | Set `daily_dd_pct=100` to disable |