//! Handler trait, per-event context, and the priority-ordered registry.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use echo_core::{ApiRequest, ApiResponse, Event, MessageEvent, Segment};
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::connection::Outgoing;
use crate::error::EchoServerError;

/// How long [`Context::send_api`] waits for the correlated response.
pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

/// Per-event handle handed to handlers.
///
/// It exposes the bot's capabilities without leaking connection internals.
#[derive(Clone)]
pub struct Context {
    /// QQ number of the bot on this connection.
    pub self_id: i64,
    /// Unique id of the WebSocket connection that delivered the event.
    pub connection_id: String,
    pub(crate) api_tx: mpsc::Sender<Outgoing>,
    pub(crate) pending: Arc<DashMap<String, oneshot::Sender<ApiResponse>>>,
    response_timeout: Duration,
}

impl Context {
    pub fn new(
        self_id: i64,
        connection_id: String,
        api_tx: mpsc::Sender<Outgoing>,
        pending: Arc<DashMap<String, oneshot::Sender<ApiResponse>>>,
    ) -> Self {
        Self {
            self_id,
            connection_id,
            api_tx,
            pending,
            response_timeout: DEFAULT_RESPONSE_TIMEOUT,
        }
    }

    /// Call an OneBot action and await the correlated response.
    pub async fn send_api(&self, mut request: ApiRequest) -> Result<ApiResponse, EchoServerError> {
        let echo = uuid::Uuid::new_v4().to_string();
        request.echo = Some(echo.clone());

        let (tx, rx) = oneshot::channel();
        self.pending.insert(echo.clone(), tx);
        // 发送失败时立刻清掉 pending 条目，避免泄漏 oneshot Sender
        if self.api_tx.send(Outgoing::Api(request)).await.is_err() {
            self.pending.remove(&echo);
            return Err(EchoServerError::ConnectionClosed);
        }

        let result = timeout(self.response_timeout, rx).await;
        self.pending.remove(&echo);

        match result {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(EchoServerError::ConnectionClosed),
            Err(_) => Err(EchoServerError::ApiTimeout),
        }
    }

    /// Send a message to a private chat.
    pub async fn send_private_msg(
        &self,
        user_id: i64,
        message: Vec<Segment>,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_api(echo_core::action::actions::send_private_msg(
            user_id, message,
        ))
        .await
    }

    /// Send a message to a group.
    pub async fn send_group_msg(
        &self,
        group_id: i64,
        message: Vec<Segment>,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_api(echo_core::action::actions::send_group_msg(
            group_id, message,
        ))
        .await
    }

    /// Send a text message to a private chat.
    pub async fn send_private_text(
        &self,
        user_id: i64,
        content: &str,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_private_msg(user_id, echo_core::message::text(content))
            .await
    }

    /// Send a text message to a group.
    pub async fn send_group_text(
        &self,
        group_id: i64,
        content: &str,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_group_msg(group_id, echo_core::message::text(content))
            .await
    }

    /// Upload a file to a group chat via the OneBot action.
    ///
    /// `file` is a path on the OneBot host (NapCat container), `name` is the
    /// file name shown in QQ.
    pub async fn upload_group_file(
        &self,
        group_id: i64,
        file: &str,
        name: &str,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_api(echo_core::action::actions::upload_group_file(
            group_id, file, name,
        ))
        .await
    }

    /// Upload a file to a private chat via the OneBot action.
    ///
    /// `file` is a path on the OneBot host (NapCat container), `name` is the
    /// file name shown in QQ.
    pub async fn upload_private_file(
        &self,
        user_id: i64,
        file: &str,
        name: &str,
    ) -> Result<ApiResponse, EchoServerError> {
        self.send_api(echo_core::action::actions::upload_private_file(
            user_id, file, name,
        ))
        .await
    }
    /// Reply to a message: private messages get a DM; group messages get a
    /// group message that mentions the sender.
    pub async fn reply_to(
        &self,
        msg: &MessageEvent,
        content: impl Into<String>,
    ) -> Result<ApiResponse, EchoServerError> {
        let content = content.into();
        match msg {
            MessageEvent::Private { user_id, .. } => {
                self.send_private_text(*user_id, &content).await
            }
            MessageEvent::Group {
                group_id, user_id, ..
            } => {
                self.send_group_msg(
                    *group_id,
                    vec![Segment::at(user_id.to_string()), Segment::text(content)],
                )
                .await
            }
            MessageEvent::Unknown => Err(EchoServerError::Handshake("unknown message event")),
        }
    }
}

/// What a handler wants to happen after it processes an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleResult {
    /// This handler consumed the event; stop dispatching.
    Handled,
    /// Not for this handler; continue with the next one.
    Pass,
    /// Handled, but still let other handlers observe the event.
    HandledAndContinue,
}

/// A unit of bot logic. Handlers are dispatched events in priority order.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Unique handler name, used in logs.
    fn name(&self) -> &str;

    /// Lower priority runs first. The default (100) is a sensible middle.
    fn priority(&self) -> i32 {
        100
    }

    /// Process one event.
    async fn handle(&self, ctx: &Context, event: &Event) -> HandleResult;
}

/// Priority-ordered collection of handlers.
pub struct HandlerRegistry {
    handlers: Vec<Arc<dyn Handler>>,
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Register a handler, keeping the registry ordered by priority.
    pub fn register<H: Handler + 'static>(&mut self, handler: H) {
        self.handlers.push(Arc::new(handler));
        self.handlers.sort_by_key(|h| h.priority());
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Registered handler names, in dispatch order.
    pub fn names(&self) -> Vec<&str> {
        self.handlers.iter().map(|h| h.name()).collect()
    }

    /// Dispatch an event to every handler until one returns [`HandleResult::Handled`].
    pub async fn dispatch(&self, ctx: &Context, event: &Event) {
        for handler in &self.handlers {
            let name = handler.name().to_string();
            // panic 隔离：单个 handler 崩溃不能拖垮整个分发任务
            let result = match std::panic::AssertUnwindSafe(handler.handle(ctx, event))
                .catch_unwind()
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    tracing::error!(handler = %name, "handler panicked while processing event");
                    return;
                }
            };
            match result {
                HandleResult::Handled => {
                    tracing::debug!(handler = %name, "event handled");
                    break;
                }
                HandleResult::HandledAndContinue => {
                    tracing::debug!(handler = %name, "event handled (continued)");
                }
                HandleResult::Pass => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Records every event it sees and replies with a fixed `HandleResult`.
    struct Recorder {
        name: &'static str,
        priority: i32,
        result: HandleResult,
        seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Handler for Recorder {
        fn name(&self) -> &str {
            self.name
        }
        fn priority(&self) -> i32 {
            self.priority
        }
        async fn handle(&self, _ctx: &Context, _event: &Event) -> HandleResult {
            self.seen.fetch_add(1, Ordering::SeqCst);
            self.result
        }
    }

    /// A handler that panics — used to verify panic isolation.
    struct Panicking;

    #[async_trait]
    impl Handler for Panicking {
        fn name(&self) -> &str {
            "panicking"
        }
        fn priority(&self) -> i32 {
            0 // runs first
        }
        async fn handle(&self, _ctx: &Context, _event: &Event) -> HandleResult {
            panic!("boom");
        }
    }

    /// A private-message event used as dispatch input.
    fn private_event() -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 42,
            "user_id": 123456,
            "message": [{"type": "text", "data": {"text": "hi"}}],
            "raw_message": "hi",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap()
    }

    /// Build a Context backed by a fresh (discarded) channel.
    fn test_context() -> (Context, mpsc::Receiver<Outgoing>) {
        let (api_tx, api_rx) = mpsc::channel(16);
        let ctx = Context::new(10001, "conn-test".into(), api_tx, Arc::new(DashMap::new()));
        (ctx, api_rx)
    }

    #[tokio::test]
    async fn dispatch_runs_lowest_priority_first() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(Recorder {
            name: "high",
            priority: 1000,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });
        registry.register(Recorder {
            name: "low",
            priority: -100,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });
        assert_eq!(registry.names(), vec!["low", "high"]);

        let (ctx, _rx) = test_context();
        registry.dispatch(&ctx, &private_event()).await;
        // "high" should never be reached because "low" handled first.
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pass_continues_to_next_handler() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(Recorder {
            name: "first",
            priority: 10,
            result: HandleResult::Pass,
            seen: seen.clone(),
        });
        registry.register(Recorder {
            name: "second",
            priority: 20,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });

        let (ctx, _rx) = test_context();
        registry.dispatch(&ctx, &private_event()).await;
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn handled_stops_dispatch() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(Recorder {
            name: "first",
            priority: 10,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });
        registry.register(Recorder {
            name: "second",
            priority: 20,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });

        let (ctx, _rx) = test_context();
        registry.dispatch(&ctx, &private_event()).await;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handled_and_continue_keeps_going() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(Recorder {
            name: "first",
            priority: 10,
            result: HandleResult::HandledAndContinue,
            seen: seen.clone(),
        });
        registry.register(Recorder {
            name: "second",
            priority: 20,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });

        let (ctx, _rx) = test_context();
        registry.dispatch(&ctx, &private_event()).await;
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn panicking_handler_does_not_crash_dispatch() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(Panicking);
        registry.register(Recorder {
            name: "after",
            priority: 100,
            result: HandleResult::Handled,
            seen: seen.clone(),
        });

        let (ctx, _rx) = test_context();
        // Must not propagate the panic.
        registry.dispatch(&ctx, &private_event()).await;
        // Panic stops dispatch entirely.
        assert_eq!(seen.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn send_api_correlates_response_by_echo() {
        let (ctx, mut api_rx) = test_context();

        // Start the call and grab the outgoing request.
        let call_ctx = ctx.clone();
        let request_task = tokio::spawn(async move {
            call_ctx
                .send_api(echo_core::action::ApiRequest {
                    action: "send_private_msg".into(),
                    params: serde_json::json!({}),
                    echo: None,
                })
                .await
        });

        let outgoing = api_rx.recv().await.expect("request queued");
        let Outgoing::Api(request) = outgoing else {
            panic!("expected Api outgoing");
        };
        let echo = request.echo.clone().expect("echo set");

        // Respond as NapCat would.
        let resp = ApiResponse {
            status: "ok".into(),
            retcode: 0,
            data: serde_json::json!({"message_id": 7}),
            echo: Some(echo.clone()),
            message: None,
            wording: None,
        };
        let (_, pending_tx) = ctx.pending.remove(&echo).expect("pending entry exists");
        pending_tx.send(resp.clone()).expect("response delivered");

        let received = request_task.await.expect("task ok").expect("send_api ok");
        assert_eq!(received, resp);
    }

    #[tokio::test]
    async fn send_api_fails_when_connection_closed() {
        // Drop the receiver so sends fail immediately.
        let (api_tx, api_rx) = mpsc::channel(16);
        drop(api_rx);
        let ctx = Context::new(10001, "conn-test".into(), api_tx, Arc::new(DashMap::new()));
        let result = ctx
            .send_api(echo_core::action::ApiRequest {
                action: "send_private_msg".into(),
                params: serde_json::json!({}),
                echo: None,
            })
            .await;
        assert!(matches!(result, Err(EchoServerError::ConnectionClosed)));
    }
}
