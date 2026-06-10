// ============================================================
// src/types.rs — Shared Domain Types
// ============================================================

use serde::{Deserialize, Serialize};

// ── Binance @bookTicker stream payload ──────────────────────
// Combined stream wraps data in: {"stream":"btcusdt@bookTicker","data":{...}}
// The "data" object contains field "s" = symbol
#[derive(Debug, Clone, Deserialize)]
pub struct BookTickerRaw {
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

// ── Parsed numeric tick — passed to Python ───────────────────
// symbol field added for multi-symbol routing
#[derive(Debug, Clone)]
pub struct Tick {
    pub symbol:  String,
    pub bid:     f64,
    pub ask:     f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub ts_ms:   i64,
}

impl Tick {
    #[inline(always)]
    pub fn spread(&self) -> f64 { self.ask - self.bid }

    #[inline(always)]
    pub fn mid(&self) -> f64 { (self.bid + self.ask) * 0.5 }
}

// ── Precision rules ──────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct PrecisionRules {
    pub price_precision: u32,
    pub qty_precision:   u32,
    pub tick_size:       f64,
    pub step_size:       f64,
    pub min_notional:    f64,
}

// ── Wallet state ─────────────────────────────────────────────
#[derive(Debug, Clone, Default)]
pub struct WalletState {
    pub balance_usdt:   f64,
    pub unrealised_pnl: f64,
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

// ── Binance REST order response ──────────────────────────────
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: u64,
    pub symbol: String,
    pub status: String,
    #[serde(rename = "type", default)]
    pub order_type: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "avgPrice", default)]
    pub avg_price: String,
    #[serde(default)]
    pub price: String,
}

// ── Error taxonomy ────────────────────────────────────────────
#[derive(thiserror::Error, Debug)]
pub enum ScalperError {
    #[error("WebSocket: {0}")]
    WebSocket(String),
    #[error("REST request: {0}")]
    Rest(#[from] reqwest::Error),
    #[error("REST API: {0}")]
    RestApi(String),
    #[error("JSON parse: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Precision rule missing for symbol {0}")]
    MissingPrecision(String),
    #[error("Order rejected: {0}")]
    OrderRejected(String),
    #[error("Rate limit exceeded")]
    RateLimit,
}