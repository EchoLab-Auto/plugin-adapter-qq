//! Per-connection read/write tasks and API-call correlation.
//!
//! Each accepted WebSocket connection is served by two tokio tasks:
//! a write task that serializes outbound API calls onto the socket, and the
//! current task's read loop that parses inbound frames. Incoming events are
//! forwarded to a per-connection dispatch task; incoming action responses are
//! matched to waiting callers via the `echo` field.
//!
//! 通道均为有界（防内存 DoS）；写失败会通知读循环立刻拆除连接；
//! teardown 时先清 pending 再带超时等待任务，避免卡死。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use echo_core::{ApiResponse, Event};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{info, warn};

use crate::error::EchoServerError;
use crate::registry::{Context, HandlerRegistry};
use crate::server::ConnectionTracker;

/// 出站 API 队列与入站事件队列的容量上限。
const CHANNEL_CAPACITY: usize = 1024;

/// Messages queued for the per-connection write task.
#[derive(Debug)]
pub enum Outgoing {
    Api(echo_core::ApiRequest),
    Ping,
    Pong(Vec<u8>),
    Close,
}

/// Serve one reverse-WebSocket connection until the peer disconnects.
///
/// Returns `Ok` on a clean disconnect, `Err` on a protocol or I/O failure.
pub async fn spawn(
    ws: WebSocketStream<TcpStream>,
    conn_id: &str,
    self_id: i64,
    client_role: &str,
    registry: Arc<HandlerRegistry>,
    tracker: ConnectionTracker,
    heartbeat_interval: Option<std::time::Duration>,
) -> Result<(), EchoServerError> {
    let (sink, stream) = ws.split();

    // Outbound channel to the write task.
    let (api_tx, api_rx) = mpsc::channel::<Outgoing>(CHANNEL_CAPACITY);
    // Correlation map: echo id -> response channel.
    let pending: Arc<DashMap<String, oneshot::Sender<ApiResponse>>> = Arc::new(DashMap::new());
    // Inbound event channel to the dispatch task.
    let (event_tx, event_rx) = mpsc::channel::<Event>(CHANNEL_CAPACITY);
    // Write failure signals the read loop to tear down promptly.
    let write_failed = Arc::new(Notify::new());

    info!(conn = %conn_id, role = %client_role, "connection tasks started");
    let write_task = tokio::spawn(write_loop(sink, api_rx, write_failed.clone()));

    let dispatch_task = {
        let api_tx = api_tx.clone();
        let pending = pending.clone();
        let registry = registry.clone();
        let conn_id = conn_id.to_string();
        tokio::spawn(async move {
            let mut rx = event_rx;
            while let Some(event) = rx.recv().await {
                let ctx = Context::new(self_id, conn_id.clone(), api_tx.clone(), pending.clone());
                registry.dispatch(&ctx, &event).await;
                tracker.count_event();
            }
        })
    };

    let read_result = read_loop(
        stream,
        &api_tx,
        &pending,
        &event_tx,
        conn_id,
        heartbeat_interval,
        &write_failed,
    )
    .await;

    // Tear down. 顺序很重要：先停止分发并清空 pending（让等待中的
    // send_api 立刻以 ConnectionClosed 失败，而不是干等超时），
    // 再关闭写循环，最后带超时回收两个任务。
    drop(event_tx);
    let _ = api_tx.try_send(Outgoing::Close);
    pending.clear();
    drop(api_tx);
    await_task(conn_id, write_task, "write").await;
    await_task(conn_id, dispatch_task, "dispatch").await;

    read_result
}

/// Join a teardown task with a deadline, logging panics and timeouts instead
/// of silently discarding them.
async fn await_task(conn_id: &str, task: tokio::task::JoinHandle<()>, name: &str) {
    match tokio::time::timeout(Duration::from_secs(5), task).await {
        Ok(Ok(())) => {}
        Ok(Err(join_err)) => {
            warn!(conn = %conn_id, %name, error = %join_err, "task panicked during teardown")
        }
        Err(_) => warn!(conn = %conn_id, %name, "task did not shut down within 5s"),
    }
}

/// Read loop: parse inbound frames, forwarding events and action responses.
async fn read_loop(
    mut stream: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    api_tx: &mpsc::Sender<Outgoing>,
    pending: &Arc<DashMap<String, oneshot::Sender<ApiResponse>>>,
    event_tx: &mpsc::Sender<Event>,
    conn_id: &str,
    heartbeat_interval: Option<std::time::Duration>,
    write_failed: &Arc<Notify>,
) -> Result<(), EchoServerError> {
    let mut missed_heartbeats = 0u32;

    loop {
        let next = match heartbeat_interval {
            Some(interval) => tokio::select! {
                _ = write_failed.notified() => {
                    warn!(conn = %conn_id, "write task failed, tearing down");
                    return Err(EchoServerError::ConnectionClosed);
                }
                res = tokio::time::timeout(interval, stream.next()) => res,
            },
            None => tokio::select! {
                _ = write_failed.notified() => {
                    warn!(conn = %conn_id, "write task failed, tearing down");
                    return Err(EchoServerError::ConnectionClosed);
                }
                res = stream.next() => Ok(res),
            },
        };

        match next {
            Err(_elapsed) => {
                // Nothing arrived in time; probe the peer.
                missed_heartbeats += 1;
                if missed_heartbeats >= 2 {
                    return Err(EchoServerError::HeartbeatTimeout);
                }
                if api_tx.try_send(Outgoing::Ping).is_err() {
                    return Err(EchoServerError::ConnectionClosed);
                }
            }
            Ok(Some(Ok(message))) => {
                missed_heartbeats = 0;
                handle_message(message, api_tx, pending, event_tx, conn_id);
            }
            Ok(Some(Err(e))) => return Err(EchoServerError::WebSocket(Box::new(e))),
            Ok(None) => {
                info!(conn = %conn_id, "connection closed by peer");
                return Ok(());
            }
        }
    }
}

/// Route one inbound WebSocket message.
fn handle_message(
    message: Message,
    api_tx: &mpsc::Sender<Outgoing>,
    pending: &Arc<DashMap<String, oneshot::Sender<ApiResponse>>>,
    event_tx: &mpsc::Sender<Event>,
    conn_id: &str,
) {
    match message {
        Message::Text(text) => {
            handle_json(&text, pending, event_tx, conn_id);
        }
        Message::Binary(bytes) => match std::str::from_utf8(&bytes) {
            Ok(text) => handle_json(text, pending, event_tx, conn_id),
            Err(_) => {
                warn!(conn = %conn_id, "binary frame ignored (MessagePack not supported)");
            }
        },
        Message::Ping(payload) => {
            let _ = api_tx.try_send(Outgoing::Pong(payload));
        }
        Message::Pong(_) => {}
        Message::Close(_) => {
            info!(conn = %conn_id, "peer requested connection close");
        }
        Message::Frame(_) => {}
    }
}

/// Parse a JSON frame: either an event (`post_type` present) or an action
/// response (`echo` present).
fn handle_json(
    text: &str,
    pending: &Arc<DashMap<String, oneshot::Sender<ApiResponse>>>,
    event_tx: &mpsc::Sender<Event>,
    conn_id: &str,
) {
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => {
            warn!(conn = %conn_id, error = %e, "invalid JSON from implementation");
            return;
        }
    };

    if value.get("post_type").is_some() {
        // 原始事件落盘（debug 用）：NapCat 的 debug 开关对上行事件无效，
        // 在框架侧留一份原文，便于排查 NapCat 吞掉的消息段（如 json 卡片）。
        {
            use std::io::Write as _;
            let mut path = std::env::temp_dir();
            path.push("echo-onebot-events.jsonl");
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                let _ = writeln!(f, "{text}");
            }
        }
        // An event.
        match serde_json::from_value::<Event>(value) {
            Ok(event) => {
                // 队列满时丢弃事件（背压），并告警
                if let Err(e) = event_tx.try_send(event) {
                    warn!(conn = %conn_id, error = %e, "event queue full, dropping event");
                }
            }
            Err(e) => {
                warn!(conn = %conn_id, error = %e, "failed to parse event");
            }
        }
    } else if value.get("echo").is_some() {
        // An action response; resolve the waiting caller.
        match serde_json::from_value::<ApiResponse>(value) {
            Ok(resp) => {
                if let Some(echo) = &resp.echo {
                    if let Some((_, tx)) = pending.remove(echo) {
                        let _ = tx.send(resp);
                    }
                }
            }
            Err(e) => {
                warn!(conn = %conn_id, error = %e, "failed to parse action response");
            }
        }
    } else {
        let snippet: String = text.chars().take(200).collect();
        warn!(conn = %conn_id, "unrecognized message from implementation: {snippet}");
    }
}

/// Write loop: serialize outgoing API calls onto the socket.
///
/// 任何写失败都会通知读循环，让整条连接尽快拆除，而不是半开挂起。
async fn write_loop(
    mut sink: futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
    mut api_rx: mpsc::Receiver<Outgoing>,
    write_failed: Arc<Notify>,
) {
    while let Some(outgoing) = api_rx.recv().await {
        match outgoing {
            Outgoing::Api(request) => {
                let text = match serde_json::to_string(&request) {
                    Ok(text) => text,
                    Err(e) => {
                        warn!(error = %e, "failed to serialize API request");
                        continue;
                    }
                };
                if let Err(e) = sink.send(Message::Text(text)).await {
                    warn!(error = %e, "write failed, closing connection");
                    write_failed.notify_waiters();
                    break;
                }
            }
            Outgoing::Ping => {
                if sink.send(Message::Ping(Vec::new())).await.is_err() {
                    write_failed.notify_waiters();
                    break;
                }
            }
            Outgoing::Pong(payload) => {
                if sink.send(Message::Pong(payload)).await.is_err() {
                    write_failed.notify_waiters();
                    break;
                }
            }
            Outgoing::Close => {
                let _ = sink.close().await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_channels() -> (
        mpsc::Sender<Outgoing>,
        mpsc::Receiver<Outgoing>,
        mpsc::Sender<Event>,
        mpsc::Receiver<Event>,
    ) {
        let (api_tx, api_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        (api_tx, api_rx, event_tx, event_rx)
    }

    #[test]
    fn event_json_is_forwarded() {
        let (_, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        handle_json(
            r#"{"post_type":"meta_event","meta_event_type":"heartbeat","time":1,"self_id":10001,"interval":5000,"status":{"online":true}}"#,
            &pending,
            &event_tx,
            "conn-test",
        );
        let ev = event_rx.try_recv().expect("event forwarded");
        assert_eq!(ev.self_id(), Some(10001));
    }

    #[test]
    fn action_response_resolves_pending_call() {
        let (_api_tx, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending: Arc<DashMap<String, oneshot::Sender<ApiResponse>>> = Arc::new(DashMap::new());
        let (resp_tx, mut resp_rx) = oneshot::channel();
        pending.insert("echo-1".into(), resp_tx);

        handle_json(
            r#"{"status":"ok","retcode":0,"data":{"message_id":7},"echo":"echo-1"}"#,
            &pending,
            &event_tx,
            "conn-test",
        );
        let resp = resp_rx.try_recv().expect("caller resolved");
        assert!(resp.is_ok());
        assert_eq!(resp.data["message_id"], 7);
        // The pending entry was removed.
        assert!(pending.get("echo-1").is_none());
        assert!(event_rx.try_recv().is_err(), "no event forwarded");
    }

    #[test]
    fn response_with_unknown_echo_is_dropped() {
        let (_, _api_rx, event_tx, _event_rx) = test_channels();
        let pending: Arc<DashMap<String, oneshot::Sender<ApiResponse>>> = Arc::new(DashMap::new());
        handle_json(
            r#"{"status":"ok","retcode":0,"data":{},"echo":"nobody-waiting"}"#,
            &pending,
            &event_tx,
            "conn-test",
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn invalid_json_is_ignored() {
        let (_, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        handle_json("not json at all", &pending, &event_tx, "conn-test");
        assert!(event_rx.try_recv().is_err());
    }

    #[test]
    fn unparseable_event_is_ignored() {
        let (_, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        // post_type present but structurally invalid.
        handle_json(
            r#"{"post_type":"message","message_type":"private"}"#,
            &pending,
            &event_tx,
            "conn-test",
        );
        assert!(event_rx.try_recv().is_err(), "malformed event dropped");
    }

    #[test]
    fn unrecognized_payload_is_ignored() {
        let (_, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        handle_json(r#"{"foo":"bar"}"#, &pending, &event_tx, "conn-test");
        assert!(event_rx.try_recv().is_err());
    }

    #[test]
    fn ping_produces_pong() {
        let (api_tx, mut api_rx, event_tx, _event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        handle_message(
            Message::Ping(vec![1, 2, 3]),
            &api_tx,
            &pending,
            &event_tx,
            "conn-test",
        );
        match api_rx.try_recv().expect("pong queued") {
            Outgoing::Pong(payload) => assert_eq!(payload, vec![1, 2, 3]),
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    #[test]
    fn invalid_utf8_binary_is_ignored() {
        let (api_tx, _api_rx, event_tx, mut event_rx) = test_channels();
        let pending = Arc::new(DashMap::new());
        handle_message(
            Message::Binary(vec![0xff, 0xfe, 0xfd]),
            &api_tx,
            &pending,
            &event_tx,
            "conn-test",
        );
        assert!(event_rx.try_recv().is_err());
    }
}
