// ============================================================
// src/lib.rs — PyO3 Module Root
//
// Single ScalperEngine manages ALL symbols:
//   • One combined WebSocket stream (up to 200 symbols)
//   • One shared rate limiter
//   • One precision map (loaded once for all symbols)
//   • One wallet state (shared balance)
//   • poll_tick() returns PyTick with .symbol field
//     so Python routes to the correct SymbolState
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
use types::{Side, Tick};

// ── PyTick — now includes symbol ──────────────────────────────
#[pyclass(frozen)]
#[derive(Clone)]
pub struct PyTick {
    #[pyo3(get)] pub symbol:  String,
    #[pyo3(get)] pub bid:     f64,
    #[pyo3(get)] pub ask:     f64,
    #[pyo3(get)] pub bid_qty: f64,
    #[pyo3(get)] pub ask_qty: f64,
    #[pyo3(get)] pub ts_ms:   i64,
    #[pyo3(get)] pub spread:  f64,
    #[pyo3(get)] pub mid:     f64,
}


impl From<Tick> for PyTick {
    fn from(t: Tick) -> Self {
        // Compute derived values BEFORE any field is moved out
        let spread = t.spread();
        let mid    = t.mid();

        PyTick {
            symbol:  t.symbol,   // move happens here — after spread/mid are captured
            bid:     t.bid,
            ask:     t.ask,
            bid_qty: t.bid_qty,
            ask_qty: t.ask_qty,
            ts_ms:   t.ts_ms,
            spread,
            mid,
        }
    }
}

#[pymethods]
impl PyTick {
    fn __repr__(&self) -> String {
        format!("PyTick(symbol={}, bid={:.5}, ask={:.5}, spread={:.5})",
            self.symbol, self.bid, self.ask, self.spread)
    }
}

// ── PyOrderResult ─────────────────────────────────────────────
#[pyclass(frozen)]
pub struct PyOrderResult {
    #[pyo3(get)] pub order_id:     u64,
    #[pyo3(get)] pub status:       String,
    #[pyo3(get)] pub order_type:   String,
    #[pyo3(get)] pub executed_qty: f64,
    #[pyo3(get)] pub avg_price:    f64,
}

// ── ScalperEngine ─────────────────────────────────────────────
/// Single engine manages all symbols via one combined WebSocket.
/// Python calls poll_tick() and dispatches by tick.symbol.
#[pyclass]
pub struct ScalperEngine {
    runtime: Arc<Runtime>,
    exec:    Arc<ExecutionEngine>,
    tick_rx: Arc<RwLock<Option<mpsc::Receiver<Tick>>>>,
}

fn parse_side(side: &str) -> PyResult<Side> {
    match side.to_uppercase().as_str() {
        "BUY"  => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other  => Err(PyValueError::new_err(format!(
            "Unknown side '{}' — use BUY or SELL", other
        ))),
    }
}

fn map_order_result(r: types::OrderResponse) -> PyOrderResult {
    let avg      = r.avg_price.parse::<f64>().unwrap_or(0.0);
    let fallback = r.price.parse::<f64>().unwrap_or(0.0);
    PyOrderResult {
        order_id:     r.order_id,
        status:       r.status,
        order_type:   r.order_type,
        executed_qty: r.executed_qty.parse().unwrap_or(0.0),
        avg_price:    if avg > 0.0 { avg } else { fallback },
    }
}

#[pymethods]
impl ScalperEngine {
    /// Create engine.
    /// symbols: list of symbols e.g. ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    #[new]
    pub fn new(
        symbols:  Vec<String>,
        api_key:  String,
        secret:   String,
    ) -> PyResult<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init();

        if symbols.is_empty() {
            return Err(PyValueError::new_err("symbols list cannot be empty"));
        }
        if symbols.len() > 200 {
            return Err(PyValueError::new_err(
                "Binance limits combined stream to 200 symbols per connection"
            ));
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        );

        let limiter = Arc::new(RateLimiter::new());
        let exec    = Arc::new(ExecutionEngine::new(api_key, secret, limiter));

        Ok(ScalperEngine {
            runtime,
            exec,
            tick_rx: Arc::new(RwLock::new(None)),
        })
    }

    /// Load precision rules (once for all symbols) + wallet, then
    /// open ONE combined WebSocket for all symbols.
    pub fn start(&self, symbols: Vec<String>) -> PyResult<()> {
        let exec = Arc::clone(&self.exec);

        // Load exchange info once — covers all 700+ symbols
        self.runtime.block_on(async {
            exec.load_precision_rules().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            exec.refresh_wallet().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;

        // Build combined stream with all symbols
        let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
        let rx = self.runtime.block_on(async {
            websocket::start_stream(&symbol_refs)
        });

        *self.tick_rx.write() = Some(rx);
        Ok(())
    }

    /// Non-blocking tick poll.
    /// PyTick.symbol tells you which coin generated this tick.
    pub fn poll_tick(&self) -> PyResult<Option<PyTick>> {
        let mut guard = self.tick_rx.write();
        let rx = guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Engine not started — call start() first"))?;
        match rx.try_recv() {
            Ok(tick)                                     => Ok(Some(PyTick::from(tick))),
            Err(mpsc::error::TryRecvError::Empty)        => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) =>
                Err(PyRuntimeError::new_err("Tick stream disconnected")),
        }
    }

    /// Blocking tick poll with timeout.
    pub fn poll_tick_blocking(&self, timeout_ms: u64) -> PyResult<Option<PyTick>> {
        let mut guard = self.tick_rx.write();
        let rx = guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Engine not started"))?;
        self.runtime.block_on(async {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
                Ok(Some(tick)) => Ok(Some(PyTick::from(tick))),
                Ok(None)       => Err(PyRuntimeError::new_err("Stream closed")),
                Err(_)         => Ok(None),
            }
        })
    }

    /// LIMIT POST_ONLY — maker entry
    pub fn place_order(
        &self, symbol: &str, side: &str, qty: f64, price: f64, reduce: bool,
    ) -> PyResult<PyOrderResult> {
        let order_side = parse_side(side)?;
        let sym        = symbol.to_string();
        let exec       = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.place_limit_order(&sym, order_side, qty, price, reduce)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }).map(map_order_result)
    }

    /// MARKET — guaranteed fill for SL/TP close
    pub fn place_market_order(
        &self, symbol: &str, side: &str, qty: f64, reduce: bool,
    ) -> PyResult<PyOrderResult> {
        let order_side = parse_side(side)?;
        let sym        = symbol.to_string();
        let exec       = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.place_market_order(&sym, order_side, qty, reduce)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }).map(map_order_result)
    }

    pub fn cancel_order(&self, symbol: &str, order_id: u64) -> PyResult<()> {
        let sym  = symbol.to_string();
        let exec = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.cancel_order(&sym, order_id)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn get_order_status(&self, symbol: &str, order_id: u64) -> PyResult<(String, f64, f64)> {
        let sym  = symbol.to_string();
        let exec = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.query_order_status(&sym, order_id)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn get_position_size(&self, symbol: &str) -> PyResult<f64> {
        let sym  = symbol.to_string();
        let exec = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.query_position_size(&sym)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn get_balance(&self) -> PyResult<f64> {
        let exec = Arc::clone(&self.exec);
        self.runtime.block_on(async {
            exec.refresh_wallet().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(self.exec.wallet.read().balance_usdt)
    }

    /// Returns {symbol: (price_precision, qty_precision, tick_size, step_size)}
    pub fn get_precision(&self) -> PyResult<HashMap<String, (u32, u32, f64, f64)>> {
        let map = self.exec.precision.read();
        Ok(map.iter().map(|(k, v)| {
            (k.clone(), (v.price_precision, v.qty_precision, v.tick_size, v.step_size))
        }).collect())
    }
}

// ── Module ────────────────────────────────────────────────────
#[pymodule]
fn binance_scalper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ScalperEngine>()?;
    m.add_class::<PyTick>()?;
    m.add_class::<PyOrderResult>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}