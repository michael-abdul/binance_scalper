// ============================================================
// src/websocket.rs — Binance Futures Combined Stream Consumer
//
// One WebSocket connection handles up to 200 symbols.
// Symbol is extracted from bookTicker "s" field and stored
// in Tick so Python can route to the correct strategy.
// ============================================================

use std::time::Duration;
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::types::{BookTickerRaw, ScalperError, Tick};

pub const TICK_CHAN_CAPACITY: usize = 8192;  // larger buffer for multi-symbol

const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_MAX_MS:  u64 = 60_000;
const PING_INTERVAL_SEC: u64 = 170;

/// Start a combined bookTicker stream for all given symbols.
/// Returns a single channel receiver — each Tick carries its symbol.
/// Supports up to 200 symbols per Binance limit.
pub fn start_stream(symbols: &[&str]) -> mpsc::Receiver<Tick> {
    let (tx, rx) = mpsc::channel::<Tick>(TICK_CHAN_CAPACITY);

    let streams: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect();

    // Binance combined stream: /stream?streams=a/b/c
    let url = format!(
        "wss://stream.binancefuture.com/stream?streams={}",
        streams.join("/")
    );

    info!("[WS] Subscribing to {} symbols in one stream", symbols.len());

    tokio::spawn(async move {
        run_stream_loop(url, tx).await;
    });

    rx
}

async fn run_stream_loop(url: String, tx: mpsc::Sender<Tick>) {
    let mut backoff_ms = RECONNECT_BASE_MS;
    loop {
        info!("[WS] Connecting → {}", url);
        match connect_and_consume(url.clone(), tx.clone()).await {
            Ok(()) => {
                info!("[WS] Stream closed cleanly — exiting");
                break;
            }
            Err(e) => {
                error!("[WS] Error: {} — reconnect in {}ms", e, backoff_ms);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
    }
}

async fn connect_and_consume(url: String, tx: mpsc::Sender<Tick>) -> Result<()> {
    let (ws_stream, response) = connect_async(url.as_str())
        .await
        .context("WebSocket handshake failed")?;

    info!("[WS] Connected — HTTP {}", response.status());

    let (mut write, mut read) = ws_stream.split();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SEC));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                debug!("[WS-ping] Sending keepalive");
                if let Err(e) = write.send(Message::Ping(vec![])).await {
                    return Err(ScalperError::WebSocket(
                        format!("Ping failed: {e}")
                    ).into());
                }
            }

            incoming = read.next() => {
                let msg = match incoming {
                    Some(Ok(m))  => m,
                    Some(Err(e)) => return Err(ScalperError::WebSocket(e.to_string()).into()),
                    None         => return Ok(()),
                };

                match msg {
                    Message::Text(text) => {
                        match parse_combined(&text) {
                            Ok(tick) => {
                                if tx.try_send(tick).is_err() {
                                    warn!("[WS] Tick channel full — dropping tick");
                                }
                            }
                            Err(e) => debug!("[WS] Parse skip: {}", e),
                        }
                    }
                    Message::Ping(payload) => {
                        if let Err(e) = write.send(Message::Pong(payload)).await {
                            error!("[WS] Pong failed: {}", e);
                        }
                    }
                    Message::Close(frame) => {
                        info!("[WS] Server closed: {:?}", frame);
                        return Err(ScalperError::WebSocket(
                            "Server sent Close frame".to_string()
                        ).into());
                    }
                    _ => {}
                }
            }
        }
    }
}

fn parse_combined(text: &str) -> Result<Tick> {
    let envelope: serde_json::Value = serde_json::from_str(text)?;

    let data = envelope
        .get("data")
        .ok_or_else(|| ScalperError::WebSocket("No 'data' field".to_string()))?;

    let raw: BookTickerRaw = serde_json::from_value(data.clone())?;

    Ok(Tick {
        symbol:  raw.symbol.to_uppercase(),   // normalise to BTCUSDT etc.
        bid:     raw.bid_price.parse().context("bid_price")?,
        ask:     raw.ask_price.parse().context("ask_price")?,
        bid_qty: raw.bid_qty.parse().context("bid_qty")?,
        ask_qty: raw.ask_qty.parse().context("ask_qty")?,
        ts_ms:   Utc::now().timestamp_millis(),
    })
}