// ============================================================
// src/types.rs — Shared Domain Types
//
// All structs are Copy/Clone where possible to avoid heap
// allocation on the hot tick path.
// ============================================================

use serde::{Deserialize, Serialize};

// ── Binance @bookTicker stream payload ──────────────────────
// Raw JSON shape:
// {
//   "u": 400900217,       // order book updateId
//   "s": "BTCUSDT",       // symbol
//   "b": "25052.50",      // best bid price
//   "B": "3.00100",       // best bid qty
//   "a": "25052.60",      // best ask price
//   "A": "0.50100"        // best ask qty
// }
#[derive(Debug, Clone, Deserialize)]
pub struct BookTickerRaw {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_price: String,
    #[serde(rename = "B")]
    pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask_price: String,
    #[serde(rename = "A")]
    pub ask_qty: String,
}

// ── Parsed, numeric tick — passed to Python ─────────────────
// Kept small (40 bytes) so Python GIL acquisition cost is
// dominated by actual work, not data copying.
#[derive(Debug, Clone, Copy)]
pub struct Tick {
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub ts_ms: i64,    // receive timestamp (milliseconds)
}

impl Tick {
    #[inline(always)]
    pub fn spread(&self) -> f64 {
        self.ask - self.bid
    }

    #[inline(always)]
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) * 0.5
    }
}

// ── Precision rules read from Binance exchange info ──────────
#[derive(Debug, Clone)]
pub struct PrecisionRules {
    pub symbol: String,
    pub price_precision: u32,   // decimal places for price
    pub qty_precision: u32,     // decimal places for quantity
    pub tick_size: f64,         // minimum price increment
    pub step_size: f64,         // minimum qty increment
    pub min_notional: f64,      // minimum order value in USDT
}

// ── Wallet / account snapshot (cached in Arc<RwLock>) ────────
#[derive(Debug, Clone, Default)]
pub struct WalletState {
    pub balance_usdt: f64,      // available margin
    pub unrealised_pnl: f64,
}

// ── Open position snapshot (one per symbol for scalper) ──────
#[derive(Debug, Clone)]
pub struct PositionState {
    pub symbol: String,
    pub side: Side,
    pub size: f64,              // in contracts / base asset
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub open_ts_ms: i64,
}

// ── Order direction ──────────────────────────────────────────
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy  => "BUY",
            Side::Sell => "SELL",
        }
    }
}

// ── Signal returned by Python brain to Rust hands ────────────
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Buy,
    Sell,
    Hold,
}

// ── Binance REST order response (subset) ─────────────────────
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: u64,
    pub symbol: String,
    pub status: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
}

// ── Error taxonomy ────────────────────────────────────────────
#[derive(thiserror::Error, Debug)]
pub enum ScalperError {
    #[error("WebSocket: {0}")]
    WebSocket(String),

    #[error("REST request: {0}")]
    Rest(#[from] reqwest::Error),

    #[error("JSON parse: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Precision rule missing for symbol {0}")]
    MissingPrecision(String),

    #[error("Order rejected: {0}")]
    OrderRejected(String),

    #[error("Rate limit exceeded")]
    RateLimit,

    #[error("Channel closed")]
    ChannelClosed,
}