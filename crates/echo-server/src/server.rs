//! Reverse-WebSocket server: accept NapCat connections and serve them.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::{accept_hdr_async, WebSocketStream};
use tracing::{info, warn};

use crate::connection;
use crate::error::EchoServerError;
use crate::registry::HandlerRegistry;

/// One accepted connection, tracked for `/status`-style handlers.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: String,
    pub self_id: i64,
    pub role: String,
    pub peer: SocketAddr,
    pub connected_at: Instant,
}

/// Runtime statistics shared with handlers.
#[derive(Debug, Clone, Default)]
pub struct ConnectionTracker {
    inner: Arc<DashMap<String, ConnectionInfo>>,
    total_events: Arc<AtomicU64>,
}

impl ConnectionTracker {
    pub(crate) fn insert(&self, info: ConnectionInfo) {
        self.inner.insert(info.id.clone(), info);
    }

    pub(crate) fn remove(&self, id: &str) {
        self.inner.remove(id);
    }

    pub(crate) fn count_event(&self) {
        self.total_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of live connections.
    pub fn active_connections(&self) -> usize {
        self.inner.len()
    }

    /// All live connections.
    pub fn connections(&self) -> Vec<ConnectionInfo> {
        self.inner.iter().map(|e| e.value().clone()).collect()
    }

    /// Total events dispatched since startup.
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to listen on, e.g. `0.0.0.0:3131`.
    pub bind_address: String,
    /// Optional token; the peer must send `Authorization: Bearer <token>`.
    pub access_token: Option<String>,
    /// How often to probe an idle connection; `None` disables probing.
    pub heartbeat_interval: Option<Duration>,
}

/// 连接状态回调：参数 (是否已连接, self_id)。由上层（TUI）订阅。
pub type ConnCallback = Arc<dyn Fn(bool, i64) + Send + Sync>;

/// The reverse-WebSocket server.
pub struct Server {
    config: ServerConfig,
    registry: Arc<HandlerRegistry>,
    tracker: ConnectionTracker,
    conn_callback: Option<ConnCallback>,
}

impl Server {
    pub fn new(
        config: ServerConfig,
        registry: Arc<HandlerRegistry>,
        tracker: ConnectionTracker,
        conn_callback: Option<ConnCallback>,
    ) -> Self {
        Self {
            config,
            registry,
            tracker,
            conn_callback,
        }
    }

    /// Bind and serve forever.
    pub async fn run(self) -> Result<(), EchoServerError> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        info!(address = %self.config.bind_address, "reverse WebSocket server listening");
        self.run_with_listener(listener).await
    }

    /// Serve on a pre-bound listener (used by tests).
    pub async fn run_with_listener(self, listener: TcpListener) -> Result<(), EchoServerError> {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    // 持续 accept 失败（如 fd 耗尽）时退避，避免忙转烧 CPU
                    warn!(error = %e, "accept failed, backing off");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            let registry = self.registry.clone();
            let tracker = self.tracker.clone();
            let config = self.config.clone();
            let conn_callback = self.conn_callback.clone();

            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(stream, peer, config, registry, tracker, conn_callback).await
                {
                    warn!(peer = %peer, error = %e, "connection ended with error");
                }
            });
        }
    }
}

/// Constant-time-ish token comparison (avoids timing side channels).
fn token_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Handle a single inbound reverse-WebSocket connection.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    config: ServerConfig,
    registry: Arc<HandlerRegistry>,
    tracker: ConnectionTracker,
    conn_callback: Option<ConnCallback>,
) -> Result<(), EchoServerError> {
    // Capture request headers during the WebSocket upgrade. Authentication is
    // enforced here too: a bad token yields a 401 before the upgrade finishes.
    let mut captured: Option<(i64, Option<String>)> = None;
    // The Err type is `tungstenite::ErrorResponse` — fixed by the vendored
    // `Callback` trait signature (`FnOnce(&Request, Response) -> Result<Response, ErrorResponse>`);
    // it cannot be boxed or shrunk at this boundary, so the lint is allowed here.
    #[allow(clippy::result_large_err)]
    let callback = |req: &Request,
                    resp: Response|
     -> Result<
        Response,
        tokio_tungstenite::tungstenite::http::Response<Option<String>>,
    > {
        let self_id = req
            .headers()
            .get("x-self-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok());
        let role = req
            .headers()
            .get("x-client-role")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let auth_ok = match &config.access_token {
            Some(token) => req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|t| token_eq(t, token))
                .unwrap_or(false),
            None => true,
        };

        // x-self-id: optional for reverse WS clients that don't send it.
        if self_id.is_none() {
            tracing::warn!("WebSocket connection without x-self-id header — NapCat reverse WS client may need configuration");
        }
        captured = Some((self_id.unwrap_or(0), role));
        if !auth_ok {
            // Returning Err(response) aborts the upgrade and sends the
            // response to the client — a 401 here.
            let mut resp = resp.map(|_| None);
            *resp.status_mut() = StatusCode::UNAUTHORIZED;
            return Err(resp);
        }
        Ok(resp)
    };

    let ws = accept_hdr_async(stream, callback)
        .await
        .map_err(|e| match e {
            TungsteniteError::Http(resp) if resp.status() == StatusCode::UNAUTHORIZED => {
                EchoServerError::AuthFailed
            }
            other => EchoServerError::WebSocket(Box::new(other)),
        })?;

    let (self_id, role) = captured.ok_or(EchoServerError::Handshake("missing request headers"))?;
    let role = role.unwrap_or_else(|| "unknown".to_string());

    info!(peer = %peer, self_id, role = %role, "reverse WebSocket connection established");

    let conn_id = uuid::Uuid::new_v4().to_string();
    tracker.insert(ConnectionInfo {
        id: conn_id.clone(),
        self_id,
        role: role.clone(),
        peer,
        connected_at: Instant::now(),
    });
    if let Some(cb) = &conn_callback {
        cb(true, self_id);
    }

    let result = connection::spawn(
        ws,
        &conn_id,
        self_id,
        &role,
        registry,
        tracker.clone(),
        config.heartbeat_interval,
    )
    .await;

    tracker.remove(&conn_id);
    if let Some(cb) = &conn_callback {
        cb(false, self_id);
    }
    result
}

// Re-exported for type clarity in connection.rs signatures.
pub type WsStream = WebSocketStream<TcpStream>;

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(id: &str) -> ConnectionInfo {
        ConnectionInfo {
            id: id.into(),
            self_id: 10001,
            role: "test".into(),
            peer: "127.0.0.1:1".parse().unwrap(),
            connected_at: Instant::now(),
        }
    }

    #[test]
    fn tracker_tracks_connection_lifecycle() {
        let tracker = ConnectionTracker::default();
        assert_eq!(tracker.active_connections(), 0);
        tracker.insert(conn("a"));
        tracker.insert(conn("b"));
        assert_eq!(tracker.active_connections(), 2);
        assert_eq!(tracker.connections().len(), 2);
        tracker.remove("a");
        assert_eq!(tracker.active_connections(), 1);
        assert_eq!(tracker.connections()[0].id, "b");
    }

    #[test]
    fn tracker_counts_events() {
        let tracker = ConnectionTracker::default();
        assert_eq!(tracker.total_events(), 0);
        tracker.count_event();
        tracker.count_event();
        tracker.count_event();
        assert_eq!(tracker.total_events(), 3);
    }

    #[test]
    fn tracker_remove_missing_is_noop() {
        let tracker = ConnectionTracker::default();
        tracker.insert(conn("a"));
        tracker.remove("nope");
        assert_eq!(tracker.active_connections(), 1);
    }

    #[test]
    fn token_eq_matches_identical() {
        assert!(token_eq("secret", "secret"));
        assert!(token_eq("", ""));
    }

    #[test]
    fn token_eq_rejects_different_lengths() {
        assert!(!token_eq("a", "ab"));
        assert!(!token_eq("abc", "ab"));
    }

    #[test]
    fn token_eq_rejects_different_content() {
        assert!(!token_eq("secret", "secrex"));
        assert!(!token_eq("abc", "xyz"));
    }

    #[test]
    fn token_eq_is_case_sensitive() {
        assert!(!token_eq("Token", "token"));
    }
}
