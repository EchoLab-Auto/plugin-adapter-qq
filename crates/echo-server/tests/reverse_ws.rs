//! Integration tests: a mock NapCat connects over reverse WebSocket and the
//! full event -> handler -> action -> response loop is exercised.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use echo_core::{ApiRequest, Event};
use echo_server::{
    ConnectionTracker, Context, HandleResult, Handler, HandlerRegistry, Server, ServerConfig,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Replies to every private message with a fixed text via the API.
struct EchoBackHandler;

#[async_trait]
impl Handler for EchoBackHandler {
    fn name(&self) -> &str {
        "echo-back"
    }

    async fn handle(&self, ctx: &Context, event: &Event) -> HandleResult {
        if let Some(msg) = event.as_message() {
            let _ = ctx.send_private_text(msg.user_id(), "pong").await;
            return HandleResult::Handled;
        }
        HandleResult::Pass
    }
}

/// Start a server on a random local port and return its address + tracker.
async fn start_server(
    registry: HandlerRegistry,
    access_token: Option<String>,
) -> (std::net::SocketAddr, ConnectionTracker) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tracker = ConnectionTracker::default();
    let server = Server::new(
        ServerConfig {
            bind_address: addr.to_string(),
            access_token,
            heartbeat_interval: None,
        },
        Arc::new(registry),
        tracker.clone(),
        None,
    );
    tokio::spawn(server.run_with_listener(listener));
    (addr, tracker)
}

/// Connect a mock NapCat with the given self id.
// Err 类型由 tokio-tungstenite 固定（`tungstenite::Error`，>128B，无法装箱——
// 调用方要匹配 `Error::Http(_)` 变体）；clippy 1.98 的 result_large_err 在此豁免。
#[allow(clippy::result_large_err)]
async fn connect_mock_napcat(
    addr: std::net::SocketAddr,
    self_id: &str,
    token: Option<&str>,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<TcpStream>,
        tungstenite::http::Response<Option<Vec<u8>>>,
    ),
    tokio_tungstenite::tungstenite::Error,
> {
    let mut request = format!("ws://{addr}/").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-self-id", HeaderValue::from_str(self_id).unwrap());
    request
        .headers_mut()
        .insert("x-client-role", HeaderValue::from_static("Universal"));
    if let Some(token) = token {
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
    }
    let stream = TcpStream::connect(addr).await.unwrap();
    client_async_with_config(request, stream, None).await
}

fn private_message_event(text: &str) -> serde_json::Value {
    json!({
        "post_type": "message",
        "message_type": "private",
        "time": 1696352000,
        "self_id": 10001,
        "sub_type": "friend",
        "message_id": 9001,
        "user_id": 20001,
        "message": [{"type": "text", "data": {"text": text}}],
        "raw_message": text,
        "font": 14,
        "sender": {"user_id": 20001, "nickname": "Alice", "sex": "female", "age": 18}
    })
}

fn heartbeat_event() -> serde_json::Value {
    json!({
        "post_type": "meta_event",
        "meta_event_type": "heartbeat",
        "time": 1696352002,
        "self_id": 10001,
        "status": {"online": true, "good": true},
        "interval": 5000
    })
}

/// Full loop: send an event, watch the handler's action request arrive,
/// respond like NapCat, then send a second event to prove correlation works.
#[tokio::test]
async fn event_dispatch_and_api_correlation() {
    let mut registry = HandlerRegistry::new();
    registry.register(EchoBackHandler);
    let (addr, _tracker) = start_server(registry, None).await;

    let (mut ws, _) = connect_mock_napcat(addr, "10001", None).await.unwrap();

    // A heartbeat must pass through without producing any output.
    ws.send(Message::Text(heartbeat_event().to_string()))
        .await
        .unwrap();

    // A message event must trigger the handler's API call.
    ws.send(Message::Text(private_message_event("hi").to_string()))
        .await
        .unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for the handler's API request")
        .unwrap()
        .unwrap();
    let Message::Text(text) = frame else {
        panic!("expected a text frame, got: {frame:?}");
    };
    let req: ApiRequest = serde_json::from_str(&text).unwrap();
    assert_eq!(req.action, "send_private_msg");
    assert_eq!(req.params["user_id"], 20001);
    let echo = req.echo.clone().expect("request must carry an echo id");

    // Reply like NapCat; the handler's send_api should complete.
    ws.send(Message::Text(
        json!({"status": "ok", "retcode": 0, "data": {"message_id": 1234}, "echo": echo})
            .to_string(),
    ))
    .await
    .unwrap();

    // A second round trip proves the correlation map still works.
    ws.send(Message::Text(private_message_event("again").to_string()))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for the second API request")
        .unwrap()
        .unwrap();
    let Message::Text(text) = frame else {
        panic!("expected a text frame");
    };
    let req2: ApiRequest = serde_json::from_str(&text).unwrap();
    assert_eq!(req2.action, "send_private_msg");
    assert_ne!(
        req2.echo,
        Some(echo),
        "each request must use a fresh echo id"
    );

    ws.send(Message::Text(
        json!({"status": "ok", "retcode": 0, "data": {"message_id": 1235}, "echo": req2.echo})
            .to_string(),
    ))
    .await
    .unwrap();
}

/// A connection with a bad (or missing) token must be rejected with 401.
#[tokio::test]
async fn rejects_connection_without_valid_token() {
    let (addr, _tracker) = start_server(HandlerRegistry::new(), Some("secret".to_string())).await;

    let err = connect_mock_napcat(addr, "10001", None)
        .await
        .expect_err("connection without token should fail");
    assert!(
        matches!(err, tokio_tungstenite::tungstenite::Error::Http(_)),
        "expected an HTTP-level handshake failure, got: {err:?}"
    );

    // With the correct token the connection succeeds.
    let (mut ws, _) = connect_mock_napcat(addr, "10001", Some("secret"))
        .await
        .expect("connection with the right token should succeed");
    ws.send(Message::Text(heartbeat_event().to_string()))
        .await
        .unwrap();
}

/// The server must tolerate an unknown `post_type` without dying.
#[tokio::test]
async fn unknown_event_is_ignored() {
    let mut registry = HandlerRegistry::new();
    registry.register(EchoBackHandler);
    let (addr, _tracker) = start_server(registry, None).await;

    let (mut ws, _) = connect_mock_napcat(addr, "10001", None).await.unwrap();
    ws.send(Message::Text(
        json!({"post_type": "fancy_new_event", "foo": 1}).to_string(),
    ))
    .await
    .unwrap();

    // The server is still alive and responds to a normal event afterwards.
    ws.send(Message::Text(
        private_message_event("still here").to_string(),
    ))
    .await
    .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("server died after unknown event")
        .unwrap()
        .unwrap();
    let Message::Text(_) = frame else {
        panic!("expected a text frame");
    };
}

/// A Pass from a low-priority handler must fall through to the next one.
#[tokio::test]
async fn handler_priority_chain_falls_through() {
    /// Passes everything — must not produce any API call.
    struct PassEverything;

    #[async_trait]
    impl Handler for PassEverything {
        fn name(&self) -> &str {
            "pass-all"
        }
        fn priority(&self) -> i32 {
            100
        }
        async fn handle(&self, _ctx: &Context, _event: &Event) -> HandleResult {
            HandleResult::Pass
        }
    }

    let mut registry = HandlerRegistry::new();
    registry.register(PassEverything);
    registry.register(EchoBackHandler); // priority 100 default, registered later
    let (addr, _tracker) = start_server(registry, None).await;

    let (mut ws, _) = connect_mock_napcat(addr, "10001", None).await.unwrap();
    ws.send(Message::Text(private_message_event("hi").to_string()))
        .await
        .unwrap();

    // The fall-through handler must still produce the API call.
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for the API request")
        .unwrap()
        .unwrap();
    let Message::Text(text) = frame else {
        panic!("expected a text frame");
    };
    let req: ApiRequest = serde_json::from_str(&text).unwrap();
    assert_eq!(req.action, "send_private_msg");
}

/// Connections are tracked while alive and removed on disconnect.
#[tokio::test]
async fn connection_tracker_tracks_lifecycle() {
    let (addr, tracker) = start_server(HandlerRegistry::new(), None).await;
    assert_eq!(tracker.active_connections(), 0);

    let (mut ws, _) = connect_mock_napcat(addr, "10001", None).await.unwrap();
    // Allow the server to register the connection.
    for _ in 0..20 {
        if tracker.active_connections() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(tracker.active_connections(), 1, "connection registered");
    let conn = &tracker.connections()[0];
    assert_eq!(conn.self_id, 10001);
    assert_eq!(conn.role, "Universal");

    // Disconnect and wait for the tracker to remove the entry.
    ws.close(None).await.unwrap();
    for _ in 0..40 {
        if tracker.active_connections() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        tracker.active_connections(),
        0,
        "connection removed on close"
    );
}
