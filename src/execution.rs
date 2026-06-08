// ============================================================
// src/execution.rs — Signed REST Order Execution
//
// Covers:
//   • HMAC-SHA256 signature per Binance SIGNED endpoint spec
//   • Precision normalization (tickSize / stepSize)
//   • LIMIT + POST_ONLY order placement (maker-fee path)
//   • Cancel / query helpers
//   • Account balance refresh
// ============================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use sha2::Sha256;
use tracing::{debug, error, info, warn};

use crate::rate_limiter::RateLimiter;
use crate::types::{
    OrderResponse, PositionState, PrecisionRules,
    ScalperError, Side, WalletState,
};

type HmacSha256 = Hmac<Sha256>;

const BASE_URL: &str = "https://testnet.binancefuture.com";
const RECV_WINDOW_MS: u64 = 5000; // tolerance for clock skew

// ── Exchange info response shapes ────────────────────────────
// We only decode what we need — serde ignores unknown fields.

#[derive(serde::Deserialize, Debug)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(serde::Deserialize, Debug)]
struct SymbolInfo {
    symbol: String,
    #[serde(rename = "pricePrecision")]
    price_precision: u32,
    #[serde(rename = "quantityPrecision")]
    quantity_precision: u32,
    filters: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize, Debug)]
struct AccountBalance {
    asset: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
    #[serde(rename = "unrealizedProfit")]
    unrealized_profit: String,
}

// ── Main execution engine ─────────────────────────────────────

pub struct ExecutionEngine {
    client:    Client,
    api_key:   String,
    secret:    String,
    limiter:   Arc<RateLimiter>,

    // Shared state written here, read by Python via lib.rs accessors
    pub wallet:    Arc<RwLock<WalletState>>,
    pub positions: Arc<RwLock<HashMap<String, PositionState>>>,
    pub precision: Arc<RwLock<HashMap<String, PrecisionRules>>>,
}

impl ExecutionEngine {
    pub fn new(api_key: String, secret: String, limiter: Arc<RateLimiter>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .https_only(true)
            .connection_verbose(false)
            .build()
            .expect("reqwest client build failed");

        Self {
            client,
            api_key,
            secret,
            limiter,
            wallet:    Arc::new(RwLock::new(WalletState::default())),
            positions: Arc::new(RwLock::new(HashMap::new())),
            precision: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── Initialisation: load exchange rules ──────────────────

    pub async fn load_precision_rules(&self) -> Result<(), ScalperError> {
        // Weight = 1 for exchangeInfo
        self.limiter.check_weight(1).map_err(|_| ScalperError::RateLimit)?;

        let url = format!("{}/fapi/v1/exchangeInfo", BASE_URL);
        let resp = self.client.get(&url).send().await?;

        let info: ExchangeInfo = resp.json().await?;

        let mut map = self.precision.write();
        for sym in &info.symbols {
            // Extract tickSize and stepSize from filters array
            let mut tick_size = 0.0_f64;
            let mut step_size = 0.0_f64;
            let mut min_notional = 5.0_f64; // Binance default minimum

            for f in &sym.filters {
                match f.get("filterType").and_then(|v| v.as_str()) {
                    Some("PRICE_FILTER") => {
                        tick_size = f["tickSize"]
                            .as_str()
                            .unwrap_or("0.01")
                            .parse()
                            .unwrap_or(0.01);
                    }
                    Some("LOT_SIZE") => {
                        step_size = f["stepSize"]
                            .as_str()
                            .unwrap_or("0.001")
                            .parse()
                            .unwrap_or(0.001);
                    }
                    Some("MIN_NOTIONAL") => {
                        min_notional = f["notional"]
                            .as_str()
                            .unwrap_or("5")
                            .parse()
                            .unwrap_or(5.0);
                    }
                    _ => {}
                }
            }

            map.insert(sym.symbol.clone(), PrecisionRules {
                symbol:          sym.symbol.clone(),
                price_precision: sym.price_precision,
                qty_precision:   sym.quantity_precision,
                tick_size,
                step_size,
                min_notional,
            });
        }

        info!("[Exec] Loaded precision rules for {} symbols", map.len());
        Ok(())
    }

    // ── Account balance refresh ───────────────────────────────

    pub async fn refresh_wallet(&self) -> Result<(), ScalperError> {
        self.limiter.check_weight(5).map_err(|_| ScalperError::RateLimit)?;

        let params = self.signed_params(&[]);
        let url = format!("{}/fapi/v2/balance?{}", BASE_URL, params);

        let resp = self.client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let balances: Vec<AccountBalance> = resp.json().await?;

        let mut w = self.wallet.write();
        for b in &balances {
            if b.asset == "USDT" {
                w.balance_usdt = b.available_balance.parse().unwrap_or(0.0);
                w.unrealised_pnl = b.unrealized_profit.parse().unwrap_or(0.0);
                debug!("[Exec] Wallet: balance={:.2} upnl={:.2}",
                       w.balance_usdt, w.unrealised_pnl);
                break;
            }
        }
        Ok(())
    }

    // ── Order placement ───────────────────────────────────────

    /// Place a LIMIT POST_ONLY maker order.
    ///
    /// `price` is the raw float from strategy — this function
    /// normalizes it before transmission.
    pub async fn place_limit_order(
        &self,
        symbol:    &str,
        side:      Side,
        raw_qty:   f64,
        raw_price: f64,
        reduce_only: bool,
    ) -> Result<OrderResponse, ScalperError> {
        // Rate check first — fail fast before any I/O
        self.limiter.check_order().map_err(|_| ScalperError::RateLimit)?;

        // Normalise price and quantity against exchange rules
        let (norm_price, norm_qty) = {
            let map = self.precision.read();
            let rules = map.get(symbol)
                .ok_or_else(|| ScalperError::MissingPrecision(symbol.to_string()))?;

            (
                normalize_price(raw_price, rules.tick_size, rules.price_precision),
                normalize_qty(raw_qty, rules.step_size, rules.qty_precision),
            )
        };

        debug!(
            "[Exec] {} {} {} @ {} (raw: {} @ {})",
            side.as_str(), symbol, norm_qty, norm_price, raw_qty, raw_price
        );

        let mut kv = vec![
            ("symbol",         symbol.to_string()),
            ("side",           side.as_str().to_string()),
            ("type",           "LIMIT".to_string()),
            ("timeInForce",    "GTX".to_string()),  // GTX = POST_ONLY (maker-only)
            ("quantity",       norm_qty.to_string()),
            ("price",          norm_price.to_string()),
        ];

        if reduce_only {
            kv.push(("reduceOnly", "true".to_string()));
        }

        let params = self.signed_params_kv(&kv);
        let url = format!("{}/fapi/v1/order", BASE_URL);

        let resp = self.client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(params)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;

        if status == StatusCode::OK || status == StatusCode::CREATED {
            let order: OrderResponse = serde_json::from_str(&body)?;
            info!("[Exec] Order placed: id={} status={}", order.order_id, order.status);
            Ok(order)
        } else {
            error!("[Exec] Order rejected HTTP {}: {}", status, body);
            Err(ScalperError::OrderRejected(body))
        }
    }

    /// Cancel an open order by ID.
    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<(), ScalperError> {
        self.limiter.check_weight(1).map_err(|_| ScalperError::RateLimit)?;

        let kv = vec![
            ("symbol",  symbol.to_string()),
            ("orderId", order_id.to_string()),
        ];
        let params = self.signed_params_kv(&kv);
        let url = format!("{}/fapi/v1/order?{}", BASE_URL, params);

        let resp = self.client
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        if resp.status().is_success() {
            info!("[Exec] Order cancelled: id={}", order_id);
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            warn!("[Exec] Cancel failed: {}", body);
            Err(ScalperError::OrderRejected(body))
        }
    }

    // ── HMAC-SHA256 signing ───────────────────────────────────

    /// Build a signed query string from key-value pairs.
    /// Appends `timestamp` and `recvWindow` automatically.
    fn signed_params_kv(&self, kv: &[(&str, String)]) -> String {
        let ts = Utc::now().timestamp_millis();

        let mut parts: Vec<String> = kv
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding_simple(v)))
            .collect();

        parts.push(format!("timestamp={}", ts));
        parts.push(format!("recvWindow={}", RECV_WINDOW_MS));

        let payload = parts.join("&");
        let sig = self.sign(&payload);

        format!("{}&signature={}", payload, sig)
    }

    /// Convenience wrapper for parameter-less signed calls.
    fn signed_params(&self, extra: &[(&str, &str)]) -> String {
        let kv: Vec<(&str, String)> = extra
            .iter()
            .map(|(k, v)| (*k, v.to_string()))
            .collect();
        self.signed_params_kv(&kv)
    }

    /// Compute HMAC-SHA256 hex signature.
    fn sign(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC accepts any key size");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

// ── Precision helpers ─────────────────────────────────────────

/// Round a price DOWN to the nearest tickSize, then format with
/// the correct number of decimal places.
///
/// Example: price=25052.567, tick_size=0.10, precision=2 → "25052.50"
pub fn normalize_price(price: f64, tick_size: f64, precision: u32) -> String {
    let normalized = if tick_size > 0.0 {
        (price / tick_size).floor() * tick_size
    } else {
        price
    };
    format!("{:.prec$}", normalized, prec = precision as usize)
}

/// Round a quantity DOWN to the nearest stepSize.
pub fn normalize_qty(qty: f64, step_size: f64, precision: u32) -> String {
    let normalized = if step_size > 0.0 {
        (qty / step_size).floor() * step_size
    } else {
        qty
    };
    format!("{:.prec$}", normalized, prec = precision as usize)
}

/// Minimal URL-safe encoding — only escapes characters that
/// Binance's HMAC verification would reject.
fn urlencoding_simple(s: &str) -> String {
    // Binance signatures are over the raw query string so we
    // must NOT double-encode. Only the few problematic chars:
    s.replace('+', "%2B").replace(' ', "%20")
}

// ── Unit tests ────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_normalization() {
        // BTC: tickSize=0.10, pricePrecision=2
        assert_eq!(normalize_price(25052.567, 0.10, 2), "25052.50");
        assert_eq!(normalize_price(25052.100, 0.10, 2), "25052.10");
        assert_eq!(normalize_price(25052.999, 0.10, 2), "25052.90");
    }

    #[test]
    fn qty_normalization() {
        // BTC: stepSize=0.001, quantityPrecision=3
        assert_eq!(normalize_qty(0.12345, 0.001, 3), "0.123");
        assert_eq!(normalize_qty(1.999,   0.001, 3), "1.999");
        assert_eq!(normalize_qty(0.0009,  0.001, 3), "0.000"); // below min — caller must guard
    }
}