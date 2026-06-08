// ============================================================
// src/rate_limiter.rs — Token-Bucket Rate Limiter
//
// Binance Futures limits:
//   • 2400 request-weight per minute (rolling 1-min window)
//   • 300 orders per minute
//   • 1200 orders per 10 seconds (burst guard)
//
// This implementation tracks WEIGHT, not raw count.
// Each REST caller declares its cost before sending.
// ============================================================

use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tracing::warn;

/// Binance documented limits for USDⓈ-M Futures
pub const WEIGHT_PER_MIN:   u32 = 2400;
pub const ORDERS_PER_MIN:   u32 = 300;
pub const ORDERS_PER_10SEC: u32 = 1200;

/// Single token-bucket window
struct Bucket {
    capacity:   u32,
    tokens:     u32,
    last_refill: Instant,
    window:     Duration,
}

impl Bucket {
    fn new(capacity: u32, window: Duration) -> Self {
        Self {
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
            window,
        }
    }

    /// Attempt to consume `cost` tokens.
    /// Refills the bucket if the window has elapsed.
    /// Returns `true` if the request is allowed.
    fn try_consume(&mut self, cost: u32) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        // Full refill when window has passed
        if elapsed >= self.window {
            self.tokens = self.capacity;
            self.last_refill = now;
        }

        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// How many milliseconds until the next full refill
    fn ms_until_refill(&self) -> u64 {
        let elapsed = Instant::now().duration_since(self.last_refill);
        if elapsed >= self.window {
            0
        } else {
            (self.window - elapsed).as_millis() as u64
        }
    }
}

/// Composite rate limiter — guards both weight and order counts.
pub struct RateLimiter {
    weight_bucket:    Mutex<Bucket>,
    order_min_bucket: Mutex<Bucket>,
    order_10s_bucket: Mutex<Bucket>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            weight_bucket:    Mutex::new(Bucket::new(WEIGHT_PER_MIN,   Duration::from_secs(60))),
            order_min_bucket: Mutex::new(Bucket::new(ORDERS_PER_MIN,   Duration::from_secs(60))),
            order_10s_bucket: Mutex::new(Bucket::new(ORDERS_PER_10SEC, Duration::from_secs(10))),
        }
    }

    /// Check + consume weight for a generic REST call.
    /// Returns `Ok(())` or `Err(wait_ms)`.
    pub fn check_weight(&self, weight: u32) -> Result<(), u64> {
        let mut b = self.weight_bucket.lock();
        if b.try_consume(weight) {
            Ok(())
        } else {
            let wait = b.ms_until_refill();
            warn!("[RateLimit] Weight budget exhausted — wait {}ms", wait);
            Err(wait)
        }
    }

    /// Check + consume budget for placing one order.
    /// New order costs weight=1 on both order buckets.
    pub fn check_order(&self) -> Result<(), u64> {
        // Check all three buckets atomically (lock ordering is stable)
        let mut w = self.weight_bucket.lock();
        let mut om = self.order_min_bucket.lock();
        let mut o10 = self.order_10s_bucket.lock();

        // New order REST call = weight 1 per Binance docs
        if !w.try_consume(1) {
            let wait = w.ms_until_refill();
            warn!("[RateLimit] Weight exhausted for order — wait {}ms", wait);
            return Err(wait);
        }
        if !om.try_consume(1) {
            // Roll back weight
            w.tokens += 1;
            let wait = om.ms_until_refill();
            warn!("[RateLimit] Order/min exhausted — wait {}ms", wait);
            return Err(wait);
        }
        if !o10.try_consume(1) {
            // Roll back both
            w.tokens += 1;
            om.tokens += 1;
            let wait = o10.ms_until_refill();
            warn!("[RateLimit] Order/10s exhausted — wait {}ms", wait);
            return Err(wait);
        }

        Ok(())
    }
}

impl Default for RateLimiter {
    fn default() -> Self { Self::new() }
}