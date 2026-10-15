//! Error types for the connection layer.

#[derive(Debug, thiserror::Error)]
pub enum EchoServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("invalid handshake: {0}")]
    Handshake(&'static str),
    #[error("authentication failed")]
    AuthFailed,
    #[error("connection closed")]
    ConnectionClosed,
    #[error("no heartbeat from peer within the timeout")]
    HeartbeatTimeout,
    #[error("API response timed out")]
    ApiTimeout,
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<tokio_tungstenite::tungstenite::Error> for EchoServerError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        // 装箱：tungstenite::Error 变体 >128B，直接内联会让 Result 过大
        // （clippy::result_large_err，rust 1.98 起默认拒绝）。
        Self::WebSocket(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_are_human_readable() {
        assert_eq!(
            EchoServerError::AuthFailed.to_string(),
            "authentication failed"
        );
        assert_eq!(
            EchoServerError::ConnectionClosed.to_string(),
            "connection closed"
        );
        assert_eq!(
            EchoServerError::HeartbeatTimeout.to_string(),
            "no heartbeat from peer within the timeout"
        );
        assert_eq!(
            EchoServerError::ApiTimeout.to_string(),
            "API response timed out"
        );
        assert_eq!(
            EchoServerError::Handshake("missing headers").to_string(),
            "invalid handshake: missing headers"
        );
    }

    #[test]
    fn io_and_json_errors_wrap_source() {
        let io = EchoServerError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(io.to_string().contains("I/O error"));
        assert!(io.to_string().contains("gone"));
        let json =
            EchoServerError::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err());
        assert!(json.to_string().starts_with("JSON error"));
    }
}
