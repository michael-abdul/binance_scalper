// ============================================================
// src/execution.rs — Signed REST Order Execution
//
// Covers:
//   • HMAC-SHA256 signature per Binance SIGNED endpoint spec
//   • Precision normalization (tickSize / stepSize)
//   • LIMIT + POST_ONLY order placement (maker-fee path)
//   • MARKET order placement (for SL/TP emergency close)
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
    OrderResponse, PrecisionRules,
    ScalperError, Side, WalletState,
};

type HmacSha256 = Hmac<Sha256>;

const BASE_URL: &str = "https://testnet.binancefuture.com";
const RECV_WINDOW_MS: u64 = 5000;

// ── Exchange info response shapes ────────────────────────────

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
    #[serde(rename = "unrealizedProfit", alias = "crossUnPnl", default)]
    unrealized_profit: String,
}

#[derive(serde::Deserialize, Debug)]
struct OrderStatusResponse {
    status: String,
    #[serde(rename = "executedQty", default)]
    executed_qty: String,
    #[serde(rename = "avgPrice", default)]
    avg_price: String,
    #[serde(default)]
    price: String,
}

// ── Main execution engine ─────────────────────────────────────

pub struct ExecutionEngine {
    client:    Client,
    api_key:   String,
    secret:    String,
    limiter:   Arc<RateLimiter>,

    pub wallet:    Arc<RwLock<WalletState>>,
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
            precision: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── Initialisation ────────────────────────────────────────

    pub async fn load_precision_rules(&self) -> Result<(), ScalperError> {
        self.limiter.check_weight(1).map_err(|_| ScalperError::RateLimit)?;

        let url = format!("{}/fapi/v1/exchangeInfo", BASE_URL);
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            return Err(ScalperError::RestApi(format!(
                "exchangeInfo failed HTTP {}: {}", status, body
            )));
        }

        let info: ExchangeInfo = serde_json::from_str(&body)?;
        let mut map = self.precision.write();

        for sym in &info.symbols {
            let mut tick_size    = 0.0_f64;
            let mut step_size    = 0.0_f64;
            let mut min_notional = 5.0_f64;

            for f in &sym.filters {
                match f.get("filterType").and_then(|v| v.as_str()) {
                    Some("PRICE_FILTER") => {
                        tick_size = f["tickSize"].as_str().unwrap_or("0.01")
                            .parse().unwrap_or(0.01);
                    }
                    Some("LOT_SIZE") => {
                        step_size = f["stepSize"].as_str().unwrap_or("0.001")
                            .parse().unwrap_or(0.001);
                    }
                    Some("MIN_NOTIONAL") => {
                        min_notional = f["notional"].as_str().unwrap_or("5")
                            .parse().unwrap_or(5.0);
                    }
                    _ => {}
                }
            }

            map.insert(sym.symbol.clone(), PrecisionRules {
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

    // ── Balance ───────────────────────────────────────────────

    pub async fn refresh_wallet(&self) -> Result<(), ScalperError> {
        self.limiter.check_weight(5).map_err(|_| ScalperError::RateLimit)?;

        let params = self.signed_params(&[]);
        let url = format!("{}/fapi/v2/balance?{}", BASE_URL, params);

        let resp = self.client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await?;

        let status = resp.status();
        let body   = resp.text().await?;

        if !status.is_success() {
            return Err(ScalperError::RestApi(format!(
                "balance failed HTTP {}: {}", status, body
            )));
        }

        let balances: Vec<AccountBalance> = serde_json::from_str(&body)
            .map_err(|e| ScalperError::RestApi(format!(
                "balance decode: {} | {}", e, body
            )))?;

        let mut w = self.wallet.write();
        for b in &balances {
            if b.asset == "USDT" {
                w.balance_usdt    = b.available_balance.parse().unwrap_or(0.0);
                w.unrealised_pnl  = b.unrealized_profit.parse().unwrap_or(0.0);
                debug!("[Exec] Wallet: balance={:.2} upnl={:.2}",
                       w.balance_usdt, w.unrealised_pnl);
                break;
            }
        }
        Ok(())
    }

    // ── LIMIT POST_ONLY (entry orders) ────────────────────────

    pub async fn place_limit_order(
        &self,
        symbol:      &str,
        side:        Side,
        raw_qty:     f64,
        raw_price:   f64,
        reduce_only: bool,
    ) -> Result<OrderResponse, ScalperError> {
        self.limiter.check_order().map_err(|_| ScalperError::RateLimit)?;

        let (norm_price, norm_qty, min_notional) = {
            let map   = self.precision.read();
            let rules = map.get(symbol)
                .ok_or_else(|| ScalperError::MissingPrecision(symbol.to_string()))?;
            (
                normalize_price(raw_price, rules.tick_size, rules.price_precision),
                normalize_qty(raw_qty, rules.step_size, rules.qty_precision),
                rules.min_notional,
            )
        };

        let notional = norm_price.parse::<f64>().unwrap_or(0.0)
            * norm_qty.parse::<f64>().unwrap_or(0.0);
        if notional < min_notional {
            return Err(ScalperError::OrderRejected(format!(
                "Notional too small: {:.4} < {:.4}", notional, min_notional
            )));
        }

        debug!("[Exec] LIMIT {} {} {} @ {}", side.as_str(), symbol, norm_qty, norm_price);

        let mut kv = vec![
            ("symbol",      symbol.to_string()),
            ("side",        side.as_str().to_string()),
            ("type",        "LIMIT".to_string()),
            ("timeInForce", "GTX".to_string()),   // POST_ONLY
            ("quantity",    norm_qty.to_string()),
            ("price",       norm_price.to_string()),
        ];
        if reduce_only { kv.push(("reduceOnly", "true".to_string())); }

        self.send_order(kv).await
    }

    // ── MARKET (SL / TP emergency close) ─────────────────────
    //
    // Market orders bypass GTX rejection — guaranteed fill.
    // Used only for closing positions; never for entry.

    pub async fn place_market_order(
        &self,
        symbol:      &str,
        side:        Side,
        raw_qty:     f64,
        reduce_only: bool,
    ) -> Result<OrderResponse, ScalperError> {
        self.limiter.check_order().map_err(|_| ScalperError::RateLimit)?;

        let norm_qty = {
            let map   = self.precision.read();
            let rules = map.get(symbol)
                .ok_or_else(|| ScalperError::MissingPrecision(symbol.to_string()))?;
            normalize_qty(raw_qty, rules.step_size, rules.qty_precision)
        };

        debug!("[Exec] MARKET {} {} {}", side.as_str(), symbol, norm_qty);

        let mut kv = vec![
            ("symbol",   symbol.to_string()),
            ("side",     side.as_str().to_string()),
            ("type",     "MARKET".to_string()),
            ("quantity", norm_qty.to_string()),
        ];
        if reduce_only { kv.push(("reduceOnly", "true".to_string())); }

        self.send_order(kv).await
    }

    // ── Shared order send path ────────────────────────────────

    async fn send_order(&self, kv: Vec<(&str, String)>) -> Result<OrderResponse, ScalperError> {
        let params = self.signed_params_kv(&kv);
        let url    = format!("{}/fapi/v1/order", BASE_URL);

        let resp = self.client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(params)
            .send().await?;

        let status = resp.status();
        let body   = resp.text().await?;

        if status == StatusCode::OK || status == StatusCode::CREATED {
            let order: OrderResponse = serde_json::from_str(&body)?;
            info!("[Exec] Order placed: symbol={} id={} status={} type={}",
                order.symbol, order.order_id, order.status, order.order_type);
            Ok(order)
        } else {
            error!("[Exec] Order rejected HTTP {}: {}", status, body);
            Err(ScalperError::OrderRejected(body))
        }
    }

    // ── Cancel ────────────────────────────────────────────────

    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<(), ScalperError> {
        self.limiter.check_weight(1).map_err(|_| ScalperError::RateLimit)?;

        let kv = vec![
            ("symbol",  symbol.to_string()),
            ("orderId", order_id.to_string()),
        ];
        let params = self.signed_params_kv(&kv);
        let url    = format!("{}/fapi/v1/order?{}", BASE_URL, params);

        let resp = self.client
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await?;

        if resp.status().is_success() {
            info!("[Exec] Order cancelled: id={}", order_id);
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            warn!("[Exec] Cancel failed: {}", body);
            Err(ScalperError::OrderRejected(body))
        }
    }

    // ── Order status query ────────────────────────────────────

    pub async fn query_order_status(
        &self,
        symbol:   &str,
        order_id: u64,
    ) -> Result<(String, f64, f64), ScalperError> {
        self.limiter.check_weight(1).map_err(|_| ScalperError::RateLimit)?;

        let kv = vec![
            ("symbol",  symbol.to_string()),
            ("orderId", order_id.to_string()),
        ];
        let params = self.signed_params_kv(&kv);
        let url    = format!("{}/fapi/v1/order?{}", BASE_URL, params);

        let resp = self.client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await?;

        let status = resp.status();
        let body   = resp.text().await?;

        if !status.is_success() {
            return Err(ScalperError::RestApi(format!(
                "query_order_status HTTP {}: {}", status, body
            )));
        }

        let order: OrderStatusResponse = serde_json::from_str(&body)?;
        let executed_qty = order.executed_qty.parse::<f64>().unwrap_or(0.0);
        let avg_price = {
            let a = order.avg_price.parse::<f64>().unwrap_or(0.0);
            if a > 0.0 { a } else { order.price.parse::<f64>().unwrap_or(0.0) }
        };

        Ok((order.status, executed_qty, avg_price))
    }

    // ── Position size query ───────────────────────────────────

    pub async fn query_position_size(&self, symbol: &str) -> Result<f64, ScalperError> {
        self.limiter.check_weight(5).map_err(|_| ScalperError::RateLimit)?;

        let params = self.signed_params(&[("symbol", symbol)]);
        let url    = format!("{}/fapi/v2/positionRisk?{}", BASE_URL, params);

        let resp = self.client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await?;

        let status = resp.status();
        let body   = resp.text().await?;

        if !status.is_success() {
            return Err(ScalperError::RestApi(format!(
                "query_position_size HTTP {}: {}", status, body
            )));
        }

        let value: serde_json::Value = serde_json::from_str(&body)?;

        let amt_val = match &value {
            serde_json::Value::Array(arr) =>
                arr.first().and_then(|v| v.get("positionAmt")),
            serde_json::Value::Object(_) =>
                value.get("positionAmt"),
            _ => None,
        };

        let position_amt = match amt_val {
            Some(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => return Err(ScalperError::RestApi(format!(
                "query_position_size unexpected shape: {}", body
            ))),
        };

        Ok(position_amt.abs())
    }

    // ── HMAC signing ──────────────────────────────────────────

    fn signed_params_kv(&self, kv: &[(&str, String)]) -> String {
        let ts = Utc::now().timestamp_millis();
        let mut parts: Vec<String> = kv.iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding_simple(v)))
            .collect();
        parts.push(format!("timestamp={}", ts));
        parts.push(format!("recvWindow={}", RECV_WINDOW_MS));
        let payload = parts.join("&");
        let sig     = self.sign(&payload);
        format!("{}&signature={}", payload, sig)
    }

    fn signed_params(&self, extra: &[(&str, &str)]) -> String {
        let kv: Vec<(&str, String)> = extra.iter()
            .map(|(k, v)| (*k, v.to_string()))
            .collect();
        self.signed_params_kv(&kv)
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC accepts any key size");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

// ── Precision helpers ─────────────────────────────────────────

pub fn normalize_price(price: f64, tick_size: f64, precision: u32) -> String {
    let n = if tick_size > 0.0 { (price / tick_size).floor() * tick_size } else { price };
    format!("{:.prec$}", n, prec = precision as usize)
}

pub fn normalize_qty(qty: f64, step_size: f64, precision: u32) -> String {
    let n = if step_size > 0.0 { (qty / step_size).floor() * step_size } else { qty };
    format!("{:.prec$}", n, prec = precision as usize)
}

fn urlencoding_simple(s: &str) -> String {
    s.replace('+', "%2B").replace(' ', "%20")
}

// ── Tests ─────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_normalization() {
        assert_eq!(normalize_price(25052.567, 0.10, 2), "25052.50");
        assert_eq!(normalize_price(25052.100, 0.10, 2), "25052.10");
        assert_eq!(normalize_price(25052.999, 0.10, 2), "25052.90");
    }

    #[test]
    fn qty_normalization() {
        assert_eq!(normalize_qty(0.12345, 0.001, 3), "0.123");
        assert_eq!(normalize_qty(1.999,   0.001, 3), "1.999");
        assert_eq!(normalize_qty(0.0009,  0.001, 3), "0.000");
    }
}