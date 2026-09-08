# v6drop - WebRTC P2P Signaling Server (Rust + Axum)

Signaling Server siêu nhẹ viết bằng **Rust**, sử dụng framework **Axum** và **Tokio WebSocket**, tối ưu hoá đặc biệt để chạy trên môi trường giới hạn tài nguyên như **Render Free (512MB RAM)** hoặc VPS nhỏ (128MB RAM).

---

## 🚀 Đặc điểm kỹ thuật & Kiến trúc

1. **Hiệu năng & Tối ưu RAM:**
   - Quản lý trạng thái bằng `Arc<RwLock<HashMap<String, Room>>>` kết hợp kênh truyền bất đồng bộ không chặn `tokio::sync::mpsc::unbounded_channel`.
   - **Zero Memory Leak:** Khi client ngắt kết nối (`drop socket`), server ngay lập tức gỡ bỏ Peer. Khi phòng không còn ai, Room sẽ bị xóa hoàn toàn khỏi RAM.
   - Cấu hình Profile Release với `opt-level = 3`, `lto = true`, `strip = true`, `panic = "abort"` giúp binary siêu nhỏ và RAM tiêu thụ thực tế chỉ vài MB.

2. **Cơ chế Room & Peer (WebRTC P2P 1-1):**
   - Giới hạn tối đa **2 Peers** trong 1 phòng (Sender & Receiver).
   - Từ chối kết nối mới (trả mã lỗi HTTP `409 Conflict`) ngay từ bước handshake nếu phòng đã đủ 2 người, tránh lãng phí RAM.
   - Tự động chuyển tiếp (forward) nguyên vẹn mọi tin nhắn Text/JSON hoặc Binary từ Peer này sang Peer kia mà không can thiệp nội dung.

3. **Keep-Alive & Chống ngủ đông (Render Anti-Sleep):**
   - **Xử lý Ping riêng biệt:** Khi client gửi tin nhắn JSON `{"type": "ping"}`, server lập tức phản hồi `{"type": "pong"}` và **không forward** sang peer còn lại, tránh làm sai lệch luồng tín hiệu WebRTC.
   - **Heartbeat định kỳ:** Server gửi WebSocket Ping protocol frame mỗi 25 giây để giữ kết nối không bị Render ngắt do idle timeout (thường là 60s).

4. **Bảo mật & Cấu hình:**
   - Hỗ trợ file `.env` qua thư viện `dotenvy` (đã cấu hình `.gitignore` an toàn cho repo public).
   - Tích hợp CORS cho phép truy cập từ mọi Origin.

---

## 📡 WebSocket API

### 1. Kết nối vào Room
```text
GET /ws?room_id=<ROOM_ID>&peer_id=<PEER_ID>
```

- **Query Parameters:**
  - `room_id`: Tên / ID định danh phòng (Ví dụ: `room_abc123`).
  - `peer_id`: ID định danh duy nhất của client (Ví dụ: `peer_sender_1`).

- **Mã phản hồi HTTP:**
  - `101 Switching Protocols`: Nâng cấp kết nối thành công.
  - `400 Bad Request`: Thiếu `room_id` hoặc `peer_id`.
  - `409 Conflict`: Phòng đã đủ 2 người kết nối.

### 2. Định dạng trao đổi dữ liệu

* **Gói tin Ping giữ kết nối (Client gửi lên Server):**
  ```json
  {"type": "ping"}
  ```
  Server phản hồi trực tiếp:
  ```json
  {"type": "pong"}
  ```

* **Gói tin WebRTC (Offer, Answer, ICE Candidate):**
  Gửi nguyên vẹn JSON giữa 2 peer:
  ```json
  {
    "type": "offer",
    "sdp": "..."
  }
  ```

### 3. Health Check
```text
GET /
GET /health
```
Trả về mã `200 OK` phục vụ cấu hình Health Check Path trên Render.

---

## 🛠️ Hướng dẫn Deploy lên Render (Native Rust - Không cần Docker)

Render hỗ trợ build Rust nguyên bản (Native Rust runtime) cực kỳ nhanh:

1. Đăng nhập vào [Render Dashboard](https://dashboard.render.com/) -> Chọn **New +** -> **Web Service**.
2. Chọn repository **`v6drop`** từ tài khoản GitHub của bạn.
3. Thiết lập thông số cấu hình:
   - **Name:** `v6drop` (hoặc tuỳ chọn)
   - **Runtime:** `Rust`
   - **Region:** Singapore (hoặc gần vị trí người dùng của bạn)
   - **Branch:** `main`
   - **Build Command:**
     ```bash
     cargo build --release
     ```
   - **Start Command:**
     ```bash
     ./target/release/v6drop
     ```
   - **Instance Type:** `Free`
4. **Environment Variables:**
   - Render sẽ tự động gán biến môi trường `PORT` (mặc định server sẽ lắng nghe trên cổng này).
   - (Tùy chọn) Thêm `RUST_LOG=v6drop=info,tower_http=info` nếu muốn xem log chi tiết.
5. Nhấn **Deploy Web Service** và hoàn tất!

---

## 💻 Chạy thử ở môi trường Local

```bash
# Clone repo
git clone https://github.com/thanhdat050625/v6drop.git
cd v6drop

# Tạo file cấu hình môi trường từ mẫu
cp .env.example .env

# Chạy server ở chế độ debug
cargo run

# Hoặc build bản tối ưu release
cargo build --release
./target/release/v6drop
```
