// ============================================================
// src/lib.rs — PyO3 Module Root
//
// Exposes the Rust engine to Python as a native extension module.
//
// Python import:
//   import binance_scalper as bs
//   engine = bs.ScalperEngine("BTCUSDT", api_key, secret)
//   engine.start()
//   while True:
//       tick = engine.poll_tick()   # non-blocking
//       ...
// ============================================================

mod execution;
mod rate_limiter;
mod types;
mod websocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use execution::ExecutionEngine;
use rate_limiter::RateLimiter;
use types::{PositionState, ScalperError, Side, Tick};

// ── Python-visible Tick struct ────────────────────────────────
// Mirrors `types::Tick` but implements IntoPy so pyo3 can
// convert it without an extra allocation.
#[pyclass(frozen)]
#[derive(Clone)]
pub struct PyTick {
    #[pyo3(get)]
    pub bid: f64,
    #[pyo3(get)]
    pub ask: f64,
    #[pyo3(get)]
    pub bid_qty: f64,
    #[pyo3(get)]
    pub ask_qty: f64,
    #[pyo3(get)]
    pub ts_ms: i64,
    #[pyo3(get)]
    pub spread: f64,
    #[pyo3(get)]
    pub mid: f64,
}

impl From<Tick> for PyTick {
    fn from(t: Tick) -> Self {
        PyTick {
            bid:     t.bid,
            ask:     t.ask,
            bid_qty: t.bid_qty,
            ask_qty: t.ask_qty,
            ts_ms:   t.ts_ms,
            spread:  t.spread(),
            mid:     t.mid(),
        }
    }
}

#[pymethods]
impl PyTick {
    fn __repr__(&self) -> String {
        format!(
            "PyTick(bid={:.5}, ask={:.5}, spread={:.5}, ts_ms={})",
            self.bid, self.ask, self.spread, self.ts_ms
        )
    }
}

// ── Order result visible to Python ───────────────────────────
#[pyclass(frozen)]
pub struct PyOrderResult {
    #[pyo3(get)]
    pub order_id: u64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub executed_qty: f64,
    #[pyo3(get)]
    pub avg_price: f64,
}

// ── Main Python-facing engine class ──────────────────────────

/// The ScalperEngine bridges the Rust async core with the
/// synchronous Python strategy layer.
///
/// Design choice: Python calls are synchronous (blocking on the
/// Rust side via `Runtime::block_on`). The WebSocket stream runs
/// in a background tokio task and pushes ticks into a channel
/// that Python drains with `poll_tick()` — no GIL contention on
/// the hot path because tick production is purely Rust.
#[pyclass]
pub struct ScalperEngine {
    symbol:   String,
    runtime:  Arc<Runtime>,
    exec:     Arc<ExecutionEngine>,
    tick_rx:  Arc<RwLock<Option<mpsc::Receiver<Tick>>>>,
}

#[pymethods]
impl ScalperEngine {
    /// Create engine. Does NOT start the stream yet.
    ///
    /// # Arguments
    /// * `symbol`  — e.g. "BTCUSDT" (Binance Futures symbol)
    /// * `api_key` — Binance API key
    /// * `secret`  — Binance secret key
    #[new]
    pub fn new(symbol: String, api_key: String, secret: String) -> PyResult<Self> {
        // Initialise tracing once (subsequent calls are no-ops)
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init();

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)   // 2 threads: WS + REST
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        );

        let limiter = Arc::new(RateLimiter::new());
        let exec = Arc::new(ExecutionEngine::new(api_key, secret, limiter));

        Ok(ScalperEngine {
            symbol,
            runtime,
            exec,
            tick_rx: Arc::new(RwLock::new(None)),
        })
    }

    /// Load exchange precision rules + wallet state, then start
    /// the WebSocket stream. Must be called before `poll_tick`.
    pub fn start(&self) -> PyResult<()> {
        let exec = Arc::clone(&self.exec);
        let symbol_lower = self.symbol.to_lowercase();

        // Load exchange info (blocking in Python context)
        self.runtime.block_on(async {
            exec.load_precision_rules().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            exec.refresh_wallet().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;

        // Spawn WebSocket stream; returns the receiver end
        let rx = websocket::start_stream(&[&symbol_lower]);

        let mut guard = self.tick_rx.write();
        *guard = Some(rx);

        Ok(())
    }

    /// Non-blocking tick poll.
    ///
    /// Returns `Some(PyTick)` if a tick is buffered, else `None`.
    /// Python strategy should call this in a tight loop.
    pub fn poll_tick(&self) -> PyResult<Option<PyTick>> {
        let mut guard = self.tick_rx.write();
        let rx = guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Engine not started — call start() first"))?;

        // try_recv never blocks — perfect for Python's main loop
        match rx.try_recv() {
            Ok(tick) => Ok(Some(PyTick::from(tick))),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Err(PyRuntimeError::new_err("Tick stream disconnected"))
            }
        }
    }

    /// Blocking tick poll — waits up to `timeout_ms` milliseconds.
    /// Useful when Python wants to yield rather than spin-loop.
    pub fn poll_tick_blocking(&self, timeout_ms: u64) -> PyResult<Option<PyTick>> {
        let mut guard = self.tick_rx.write();
        let rx = guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Engine not started"))?;

        self.runtime.block_on(async {
            match tokio::time::timeout(
                Duration::from_millis(timeout_ms),
                rx.recv(),
            ).await {
                Ok(Some(tick)) => Ok(Some(PyTick::from(tick))),
                Ok(None)       => Err(PyRuntimeError::new_err("Stream closed")),
                Err(_)         => Ok(None),  // timeout → None (not an error)
            }
        })
    }

    /// Place a LIMIT POST_ONLY order. Returns order id.
    ///
    /// # Arguments
    /// * `side`   — "BUY" or "SELL"
    /// * `qty`    — raw quantity float (will be normalized)
    /// * `price`  — raw limit price float (will be normalized)
    /// * `reduce` — if True, sets reduceOnly=true (for closing)
    pub fn place_order(
        &self,
        side:   &str,
        qty:    f64,
        price:  f64,
        reduce: bool,
    ) -> PyResult<PyOrderResult> {
        let order_side = match side.to_uppercase().as_str() {
            "BUY"  => Side::Buy,
            "SELL" => Side::Sell,
            other  => return Err(PyValueError::new_err(
                format!("Unknown side '{}' — use BUY or SELL", other)
            )),
        };

        let symbol = self.symbol.clone();
        let exec   = Arc::clone(&self.exec);

        self.runtime.block_on(async {
            exec.place_limit_order(&symbol, order_side, qty, price, reduce)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
        .map(|r| PyOrderResult {
            order_id:     r.order_id,
            status:       r.status,
            executed_qty: r.executed_qty.parse().unwrap_or(0.0),
            avg_price:    r.avg_price.parse().unwrap_or(0.0),
        })
    }

    /// Cancel an open order by ID.
    pub fn cancel_order(&self, order_id: u64) -> PyResult<()> {
        let symbol = self.symbol.clone();
        let exec   = Arc::clone(&self.exec);

        self.runtime.block_on(async {
            exec.cancel_order(&symbol, order_id)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Refresh and return current USDT wallet balance.
    pub fn get_balance(&self) -> PyResult<f64> {
        let exec = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.refresh_wallet().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(self.exec.wallet.read().balance_usdt)
    }

    /// Return price precision rules as a Python dict.
    pub fn get_precision(&self) -> PyResult<HashMap<String, (u32, u32, f64, f64)>> {
        let map = self.exec.precision.read();
        Ok(map.iter().map(|(k, v)| {
            (k.clone(), (v.price_precision, v.qty_precision, v.tick_size, v.step_size))
        }).collect())
    }
}

// ── Module registration ───────────────────────────────────────

#[pymodule]
fn binance_scalper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ScalperEngine>()?;
    m.add_class::<PyTick>()?;
    m.add_class::<PyOrderResult>()?;

    // Expose version string
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}