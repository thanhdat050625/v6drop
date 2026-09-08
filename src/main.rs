use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, info, warn};

type PeerSender = mpsc::UnboundedSender<Message>;

struct Room {
    peers: HashMap<String, PeerSender>,
}

type AppState = Arc<RwLock<HashMap<String, Room>>>;

#[derive(Deserialize, Debug)]
struct WsParams {
    room_id: String,
    peer_id: String,
}

#[derive(Deserialize)]
struct PingCheck {
    r#type: Option<String>,
}

/// Kiểm tra xem message gửi lên có phải là gói Ping giữ kết nối / chống ngủ đông không
fn is_ping_message(text: &str) -> bool {
    if let Ok(msg) = serde_json::from_str::<PingCheck>(text) {
        if let Some(t) = msg.r#type {
            return t.eq_ignore_ascii_case("ping");
        }
    }
    false
}

#[tokio::main]
async fn main() {
    // Nạp cấu hình từ .env nếu tồn tại
    dotenvy::dotenv().ok();

    // Khởi tạo logger siêu nhẹ qua tracing (tắt in giờ vì Render đã tự động thêm timestamp)
    tracing_subscriber::fmt()
        .without_time()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "v6drop=info,tower_http=info".into()),
        )
        .init();

    // Khởi tạo bộ nhớ chia sẻ State chứa các Rooms
    let state: AppState = Arc::new(RwLock::new(HashMap::new()));

    // Cấu hình CORS cho phép mọi Origin truy cập
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Xây dựng router
    let app = Router::new()
        .route("/", get(health_check))
        .route("/health", get(health_check))
        .route("/ws", get(ws_handler))
        .layer(cors)
        .with_state(state);

    // Đọc port từ biến môi trường PORT (mặc định 10000 theo chuẩn Render)
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🚀 WebRTC Signaling Server listening on http://{}", addr);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("❌ Failed to bind to {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("❌ Server fatal error: {}", e);
    }
}

/// Endpoint kiểm tra sức khỏe server (Render Health Check)
async fn health_check() -> (StatusCode, &'static str) {
    (StatusCode::OK, "v6drop signaling server is running healthy!")
}

/// Handler xử lý WebSocket Upgrade
async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let room_id = params.room_id.trim();
    let peer_id = params.peer_id.trim();

    if room_id.is_empty() || peer_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "Cần truyền đủ room_id và peer_id qua query params: /ws?room_id=...&peer_id=...",
        )
            .into_response();
    }

    // Kiểm tra số lượng Peer trước khi nâng cấp handshake để tiết kiệm RAM
    {
        let rooms = state.read().await;
        if let Some(room) = rooms.get(room_id) {
            if room.peers.len() >= 2 && !room.peers.contains_key(peer_id) {
                warn!(
                    "Từ chối kết nối: Phòng '{}' đã đủ 2 peers (tối đa 2 người)",
                    room_id
                );
                return (
                    StatusCode::CONFLICT,
                    "Phòng đã đủ 2 người (Sender & Receiver), không thể tham gia thêm",
                )
                    .into_response();
            }
        }
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state, params))
}

/// Xử lý vòng đời WebSocket và Forwarding tin nhắn WebRTC
async fn handle_socket(socket: WebSocket, state: AppState, params: WsParams) {
    let room_id = params.room_id;
    let peer_id = params.peer_id;

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Đăng ký Peer vào Room
    {
        let mut rooms = state.write().await;
        let room = rooms.entry(room_id.clone()).or_insert_with(|| Room {
            peers: HashMap::new(),
        });

        // Kiểm tra lại sau khi lock write đề phòng race condition
        if room.peers.len() >= 2 && !room.peers.contains_key(&peer_id) {
            warn!("Phòng '{}' vừa đầy trước khi hoàn tất đăng ký", room_id);
            let _ = ws_sender
                .send(Message::Text(
                    r#"{"type":"error","message":"Phòng đã đầy"}"#.to_string(),
                ))
                .await;
            return;
        }

        // Lưu sender của Peer vào Room
        room.peers.insert(peer_id.clone(), tx.clone());
        info!(
            "➕ Peer '{}' đã tham gia phòng '{}' (Tổng số peer trong phòng: {})",
            peer_id,
            room_id,
            room.peers.len()
        );
    }

    // Task gửi dữ liệu từ channel mpsc ra socket của client
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Heartbeat nhẹ định kỳ mỗi 25 giây để chống timeout / giữ kết nối trên Render
    let heartbeat_tx = tx.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(25));
        loop {
            interval.tick().await;
            // Gửi WebSocket Ping protocol frame
            if heartbeat_tx.send(Message::Ping(vec![])).is_err() {
                break;
            }
        }
    });

    // Vòng lặp nhận dữ liệu từ Client socket
    let read_room_id = room_id.clone();
    let read_peer_id = peer_id.clone();
    let read_state = Arc::clone(&state);
    let responder_tx = tx.clone();

    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(msg) => match msg {
                Message::Text(text) => {
                    // 1. Xử lý Keep-Alive chống ngủ đông riêng biệt:
                    // Client gửi {"type": "ping"} -> server lập tức đáp {"type": "pong"} và KHÔNG forward
                    if is_ping_message(&text) {
                        let _ = responder_tx.send(Message::Text(r#"{"type":"pong"}"#.to_string()));
                        continue;
                    }

                    // 2. Chuyển tiếp (forward) nguyên vẹn Text/JSON sang Peer còn lại trong Room
                    let rooms = read_state.read().await;
                    if let Some(room) = rooms.get(&read_room_id) {
                        for (target_peer, target_tx) in &room.peers {
                            if target_peer != &read_peer_id {
                                let _ = target_tx.send(Message::Text(text.clone()));
                            }
                        }
                    }
                }
                Message::Binary(bin) => {
                    // Chuyển tiếp dữ liệu nhị phân nguyên vẹn (nếu có dùng WebRTC DataChannel chunk)
                    let rooms = read_state.read().await;
                    if let Some(room) = rooms.get(&read_room_id) {
                        for (target_peer, target_tx) in &room.peers {
                            if target_peer != &read_peer_id {
                                let _ = target_tx.send(Message::Binary(bin.clone()));
                            }
                        }
                    }
                }
                Message::Ping(payload) => {
                    // Phản hồi Pong tự động theo chuẩn WebSocket
                    let _ = responder_tx.send(Message::Pong(payload));
                }
                Message::Pong(_) => {
                    // Client đã nhận heartbeat
                }
                Message::Close(_) => {
                    break;
                }
            },
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Connection reset without closing handshake")
                    || err_str.contains("connection closed before message completed")
                    || err_str.contains("Connection reset by peer")
                    || err_str.contains("reset without closing")
                {
                    debug!("Peer '{}' ngắt kết nối (tắt tab/khóa máy): {}", read_peer_id, err_str);
                } else {
                    warn!("WebSocket read error trên peer '{}': {}", read_peer_id, err_str);
                }
                break;
            }
        }
    }

    // Dừng các background tasks của peer này
    heartbeat_task.abort();
    write_task.abort();

    // DỌN DẸP BỘ NHỚ NGAY LẬP TỨC (Chống rò rỉ RAM):
    // Xóa peer khỏi room. Nếu room rỗng, xóa hoàn toàn Room khỏi HashMap.
    {
        let mut rooms = state.write().await;
        if let Some(room) = rooms.get_mut(&room_id) {
            room.peers.remove(&peer_id);
            info!(
                "➖ Peer '{}' đã rời phòng '{}' (Còn lại: {})",
                peer_id,
                room_id,
                room.peers.len()
            );

            if room.peers.is_empty() {
                rooms.remove(&room_id);
                info!("🧹 Phòng '{}' đã rỗng và được giải phóng hoàn toàn khỏi RAM", room_id);
            }
        }
    }
}
