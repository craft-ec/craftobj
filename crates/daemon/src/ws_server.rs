//! WebSocket JSON-RPC 2.0 server
//!
//! Accepts WebSocket connections at `/ws` and routes JSON-RPC requests
//! through the same `IpcHandler` used by the Unix socket IPC server.
//! Connections must authenticate via `?key=<api_key>` query parameter.
//!
//! Server-push: each connection also receives `DaemonEvent`s as JSON-RPC
//! notifications with `method: "event"`.

use std::sync::Arc;

use craftec_ipc::server::IpcHandler;
use craftec_ipc::protocol::{RpcRequest, RpcResponse};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::events::EventSender;

/// Run the WebSocket JSON-RPC server.
///
/// Listens on the given port and upgrades HTTP connections at `/ws` to WebSocket.
/// Each incoming text message is parsed as a JSON-RPC 2.0 request and dispatched
/// to the shared `handler`. Connections must include `?key=<api_key>` in the URI.
///
/// Each connection also receives broadcast `DaemonEvent`s as JSON-RPC notifications.
pub async fn run_ws_server(
    port: u16,
    handler: Arc<dyn IpcHandler>,
    api_key: String,
    event_tx: EventSender,
) -> std::io::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("WebSocket server listening on ws://0.0.0.0:{}/ws", port);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let handler = handler.clone();
                let api_key = api_key.clone();
                let event_rx = event_tx.subscribe();
                tokio::spawn(async move {
                    // Perform WebSocket handshake with path + API key check
                    let ws_stream = match tokio_tungstenite::accept_hdr_async(
                        stream,
                        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            let uri = req.uri();
                            let path = uri.path();
                            if path != "/ws" && path != "/ws/" {
                                let mut err = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some("Not Found".into()));
                                *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::NOT_FOUND;
                                return Err(err);
                            }

                            // Validate API key from query string
                            let query = uri.query().unwrap_or("");
                            let key_value = query
                                .split('&')
                                .find_map(|pair| {
                                    let (k, v) = pair.split_once('=')?;
                                    if k == "key" { Some(v.to_string()) } else { None }
                                });

                            match key_value {
                                Some(ref k) if k == &api_key => Ok(resp),
                                _ => {
                                    let mut err = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some("Unauthorized".into()));
                                    *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                                    Err(err)
                                }
                            }
                        },
                    )
                    .await
                    {
                        Ok(ws) => ws,
                        Err(e) => {
                            debug!("WebSocket handshake failed from {}: {}", peer, e);
                            return;
                        }
                    };

                    debug!("WebSocket client connected: {}", peer);
                    handle_ws_connection(ws_stream, handler, peer, event_rx).await;
                    debug!("WebSocket client disconnected: {}", peer);
                });
            }
            Err(e) => {
                error!("Failed to accept TCP connection: {}", e);
            }
        }
    }
}

async fn handle_ws_connection(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    handler: Arc<dyn IpcHandler>,
    peer: std::net::SocketAddr,
    mut event_rx: crate::events::EventReceiver,
) {
    let (mut sink, mut stream) = ws_stream.split();

    loop {
        tokio::select! {
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        debug!("WebSocket read error from {}: {}", peer, e);
                        break;
                    }
                    None => break,
                };

                match msg {
                    Message::Text(text) => {
                        let response = match serde_json::from_str::<RpcRequest>(&text) {
                            Ok(req) => {
                                debug!("WS RPC: {} (id={})", req.method, req.id);
                                match handler.handle(&req.method, req.params).await {
                                    Ok(result) => RpcResponse::success(req.id, result),
                                    Err(msg) => RpcResponse::error(req.id, -32000, msg),
                                }
                            }
                            Err(e) => {
                                warn!("Invalid JSON-RPC from {}: {}", peer, e);
                                RpcResponse::error(0, -32700, format!("Parse error: {}", e))
                            }
                        };

                        let json = serde_json::to_string(&response).unwrap_or_default();
                        if sink.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Message::Ping(data) => {
                        if sink.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {} // Ignore binary, pong, etc.
                }
            }

            event = event_rx.recv() => {
                match event {
                    Ok(daemon_event) => {
                        // Send as JSON-RPC notification (no id)
                        let notification = serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": "event",
                            "params": daemon_event,
                        });
                        let json = serde_json::to_string(&notification).unwrap_or_default();
                        if sink.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client {} lagged, dropped {} events", peer, n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }
}
