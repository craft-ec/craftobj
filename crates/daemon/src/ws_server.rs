//! WebSocket JSON-RPC 2.0 server
//!
//! Accepts WebSocket connections at `/ws` and routes JSON-RPC requests
//! through the same `IpcHandler` used by the Unix socket IPC server.

use std::sync::Arc;

use craftec_ipc::server::IpcHandler;
use craftec_ipc::protocol::{RpcRequest, RpcResponse};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

/// Run the WebSocket JSON-RPC server.
///
/// Listens on the given port and upgrades HTTP connections at `/ws` to WebSocket.
/// Each incoming text message is parsed as a JSON-RPC 2.0 request and dispatched
/// to the shared `handler`.
pub async fn run_ws_server(
    port: u16,
    handler: Arc<dyn IpcHandler>,
) -> std::io::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("WebSocket server listening on ws://0.0.0.0:{}/ws", port);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let handler = handler.clone();
                tokio::spawn(async move {
                    // Perform WebSocket handshake with a path check callback
                    let ws_stream = match tokio_tungstenite::accept_hdr_async(
                        stream,
                        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            let path = req.uri().path();
                            if path == "/ws" || path == "/ws/" {
                                Ok(resp)
                            } else {
                                use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
                                let mut err = ErrorResponse::new(Some("Not Found".into()));
                                *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::NOT_FOUND;
                                Err(err)
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
                    handle_ws_connection(ws_stream, handler, peer).await;
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
) {
    let (mut sink, mut stream) = ws_stream.split();

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                debug!("WebSocket read error from {}: {}", peer, e);
                break;
            }
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
}
