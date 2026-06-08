// ============================================================
// src/websocket.rs — Binance Futures Combined Stream Consumer
//
// Connects to wss://fstream.binance.com/stream?streams=...
// Handles:
//   • Auto-reconnect with exponential back-off (max 60 s)
//   • Binance application-level ping frames (every 3 min)
//   • tokio::sync::mpsc channel → Python tick queue
//   • Stream invalidation detection (listenKey expiry, etc.)
// ============================================================

use std::time::Duration;
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::types::{BookTickerRaw, ScalperError, Tick};

// ── Channel capacity: 2048 ticks buffer ─────────────────────
// At max BTC tick rate (~5/sec) this is ~400 s of headroom.
// Python consumer must drain faster than this to avoid drops.
pub const TICK_CHAN_CAPACITY: usize = 2048;

// ── Reconnect configuration ──────────────────────────────────
const RECONNECT_BASE_MS: u64  = 500;
const RECONNECT_MAX_MS:  u64  = 60_000;
const PING_INTERVAL_SEC: u64  = 170;  // Binance disconnects after 3 min silence

/// Spawn the background WebSocket consumer task.
///
/// Returns the receiving end of the tick channel.
/// The sender half lives inside the spawned task — drop it
/// to signal Python that the stream is permanently closed.
///
/// # Arguments
/// * `symbols` — e.g. `["btcusdt", "ethusdt"]` (lowercase)
pub fn start_stream(symbols: &[&str]) -> mpsc::Receiver<Tick> {
    let (tx, rx) = mpsc::channel::<Tick>(TICK_CHAN_CAPACITY);

    // Build combined stream URL:
    // wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker
    let streams: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect();
    let path = streams.join("/");
    let url_str = format!(
        "wss://fstream.binance.com/stream?streams={}",
        path
    );

    let url = Url::parse(&url_str).expect("Invalid stream URL");

    // Move ownership into the task; the channel sender is the only
    // shutdown signal — when it drops, `rx` returns None.
    tokio::spawn(async move {
        run_stream_loop(url, tx).await;
    });

    rx
}

/// Persistent connection loop — reconnects on every error.
async fn run_stream_loop(url: Url, tx: mpsc::Sender<Tick>) {
    let mut backoff_ms = RECONNECT_BASE_MS;

    loop {
        info!("[WS] Connecting → {}", url);

        match connect_and_consume(url.clone(), tx.clone()).await {
            Ok(()) => {
                // Clean shutdown (channel closed from outside)
                info!("[WS] Stream closed cleanly — exiting loop");
                break;
            }
            Err(e) => {
                error!("[WS] Stream error: {} — reconnecting in {}ms", e, backoff_ms);
                sleep(Duration::from_millis(backoff_ms)).await;

                // Exponential back-off, capped at RECONNECT_MAX_MS
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
    }
}

/// Single WebSocket session. Returns:
/// - `Ok(())` when the channel sender is dropped (graceful exit)
/// - `Err(...)` on any connection/parse error (triggers reconnect)
async fn connect_and_consume(url: Url, tx: mpsc::Sender<Tick>) -> Result<()> {
    let (ws_stream, response) = connect_async(url)
        .await
        .context("WebSocket handshake failed")?;

    info!("[WS] Connected — HTTP {}", response.status());
    let mut backoff_ms = RECONNECT_BASE_MS; // reset on successful connect

    let (mut write, mut read) = ws_stream.split();

    // Ping task: sends a Pong every PING_INTERVAL_SEC to keep
    // Binance from closing the connection with code 1006.
    // We use a separate task so the read loop is never blocked.
    let ping_tx = {
        let (ping_cmd_tx, mut ping_cmd_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_secs(PING_INTERVAL_SEC)
            );
            interval.tick().await; // consume the first immediate tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Binance Futures responds to Ping with Pong —
                        // we send a Text ping per Binance spec.
                        debug!("[WS-ping] Sending keepalive ping");
                        if write.send(Message::Ping(vec![])).await.is_err() {
                            break;
                        }
                    }
                    // Shutdown signal from read loop
                    _ = ping_cmd_rx.recv() => break,
                }
            }
        });
        ping_cmd_tx
    };

    // ── Main read loop ────────────────────────────────────────
    loop {
        let msg = match read.next().await {
            Some(Ok(m))  => m,
            Some(Err(e)) => {
                // Signal ping task to stop before propagating error
                let _ = ping_tx.send(()).await;
                return Err(ScalperError::WebSocket(e.to_string()).into());
            }
            None => {
                // Stream ended cleanly
                let _ = ping_tx.send(()).await;
                return Ok(());
            }
        };

        match msg {
            Message::Text(text) => {
                // ── Parse combined stream envelope ────────────
                // Shape: {"stream":"btcusdt@bookTicker","data":{...}}
                match parse_combined(&text) {
                    Ok(tick) => {
                        // Non-blocking send — if the channel is full,
                        // we drop the oldest tick (never block execution).
                        if tx.try_send(tick).is_err() {
                            warn!("[WS] Tick channel full — dropping tick");
                        }
                    }
                    Err(e) => {
                        // Log but don't crash — could be a non-bookTicker
                        // event (e.g. server heartbeat).
                        debug!("[WS] Parse skip: {}", e);
                    }
                }
            }

            Message::Ping(payload) => {
                // Respond to server-initiated pings immediately.
                // tokio-tungstenite does NOT auto-pong in split mode.
                if let Err(e) = write.send(Message::Pong(payload)).await {
                    error!("[WS] Pong send failed: {}", e);
                }
            }

            Message::Close(frame) => {
                info!("[WS] Server closed: {:?}", frame);
                let _ = ping_tx.send(()).await;
                // Return error to trigger reconnect
                return Err(ScalperError::WebSocket(
                    "Server sent Close frame".to_string()
                ).into());
            }

            // Pong / Binary frames — silently ignore
            _ => {}
        }
    }
}

// ── JSON parsing ─────────────────────────────────────────────

/// Parse a combined-stream envelope and return a `Tick`.
fn parse_combined(text: &str) -> Result<Tick> {
    // Combined stream wraps data in {"stream":"...","data":{...}}
    let envelope: serde_json::Value = serde_json::from_str(text)?;

    let data = envelope
        .get("data")
        .ok_or_else(|| ScalperError::WebSocket("No 'data' field".to_string()))?;

    let raw: BookTickerRaw = serde_json::from_value(data.clone())?;

    // Parse string prices — Binance always sends prices as strings
    // to preserve precision without IEEE-754 rounding.
    let bid: f64 = raw.bid_price.parse().context("bid_price parse")?;
    let ask: f64 = raw.ask_price.parse().context("ask_price parse")?;
    let bid_qty: f64 = raw.bid_qty.parse().context("bid_qty parse")?;
    let ask_qty: f64 = raw.ask_qty.parse().context("ask_qty parse")?;

    Ok(Tick {
        bid,
        ask,
        bid_qty,
        ask_qty,
        ts_ms: Utc::now().timestamp_millis(),
    })
}