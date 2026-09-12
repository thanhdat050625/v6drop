package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Giới hạn tối đa 32 chunk trong RAM (32 * 256KB = 8.0MB RAM)
// Cho phép truyền streaming liên tục (continuous pipelining), tận dụng tối đa băng thông TCP
const MaxChunksInRam = 32

const writeWait = 20 * time.Second

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 1024,
	WriteBufferSize: 1024 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// SafeConn bọc websocket.Conn với sync.Mutex để đảm bảo 100% thread-safe cho WriteMessage
type SafeConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewSafeConn(conn *websocket.Conn) *SafeConn {
	return &SafeConn{conn: conn}
}

func (s *SafeConn) WriteMessage(messageType int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return s.conn.WriteMessage(messageType, data)
}

func (s *SafeConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

type RelayMessage struct {
	MsgType int
	Data    []byte
}

type Room struct {
	Sender   *SafeConn
	Receiver *SafeConn
	Queue    chan RelayMessage // Channel buffer giới hạn đúng 5 chunk trong RAM
	Lock     sync.Mutex
}

var (
	rooms     = make(map[string]*Room)
	roomsLock sync.Mutex
)

func getRoom(id string) *Room {
	roomsLock.Lock()
	defer roomsLock.Unlock()
	if r, exists := rooms[id]; exists {
		return r
	}
	r := &Room{
		Queue: make(chan RelayMessage, MaxChunksInRam),
	}
	rooms[id] = r
	return r
}

// Xả sạch hàng đợi khi bên nhận ngắt kết nối để giải phóng RAM ngay lập tức
func drainQueue(ch chan RelayMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func removePeerFromRoom(roomID, role string, sConn *SafeConn) {
	roomsLock.Lock()
	defer roomsLock.Unlock()

	room, exists := rooms[roomID]
	if !exists {
		return
	}

	room.Lock.Lock()
	if role == "sender" && room.Sender == sConn {
		room.Sender = nil
		log.Printf("[%s] Sender ngắt kết nối\n", roomID)
		if room.Receiver != nil {
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"sender"}`))
		}
	} else if role == "receiver" && room.Receiver == sConn {
		room.Receiver = nil
		log.Printf("[%s] Receiver ngắt kết nối\n", roomID)
		if room.Sender != nil {
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"receiver"}`))
		}
		drainQueue(room.Queue)
	}

	if room.Sender == nil && room.Receiver == nil {
		drainQueue(room.Queue)
		delete(rooms, roomID)
		log.Printf("[%s] Phòng đã rỗng -> Giải phóng bộ nhớ hoàn toàn (Zero Memory Leak)\n", roomID)
	}
	room.Lock.Unlock()
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Lỗi WS Upgrade: %v\n", err)
		return
	}

	conn.SetReadLimit(10 * 1024 * 1024)

	sConn := NewSafeConn(conn)
	defer sConn.Close()

	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		roomID = r.URL.Query().Get("room_id")
	}
	role := r.URL.Query().Get("role") // "sender" hoặc "receiver"
	if roomID == "" || role == "" {
		log.Println("Thiếu tham số room hoặc role trong URL query")
		return
	}

	room := getRoom(roomID)

	room.Lock.Lock()
	if role == "sender" {
		if room.Sender != nil {
			_ = room.Sender.Close()
		}
		room.Sender = sConn
		log.Printf("[%s] Bên Gửi (Sender) đã kết nối\n", roomID)
		if room.Receiver != nil {
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"sender"}`))
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"receiver"}`))
		}
	} else {
		if room.Receiver != nil {
			_ = room.Receiver.Close()
		}
		room.Receiver = sConn
		log.Printf("[%s] Bên Nhận (Receiver) đã kết nối\n", roomID)
		if room.Sender != nil {
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"receiver"}`))
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"sender"}`))
		}

		// Kênh dừng riêng biệt cho goroutine truyền chunk của Receiver này
		rcvStop := make(chan struct{})
		defer close(rcvStop)

		// Goroutine chuyên trách đọc từ Queue (tối đa 32 chunk) và gửi ra Receiver liên tục
		go func(rcvConn *SafeConn, q chan RelayMessage, stop chan struct{}) {
			for {
				select {
				case msg, ok := <-q:
					if !ok {
						return
					}
					if err := rcvConn.WriteMessage(msg.MsgType, msg.Data); err != nil {
						log.Printf("[%s] Lỗi gửi chunk tới Receiver: %v -> Ngắt kết nối Receiver\n", roomID, err)
						_ = rcvConn.Close()
						return
					}
				case <-stop:
					return
				}
			}
		}(sConn, room.Queue, rcvStop)
	}
	room.Lock.Unlock()

	defer removePeerFromRoom(roomID, role, sConn)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Xử lý ping giữ kết nối chống ngủ đông trên Render Free
		if msgType == websocket.TextMessage && string(data) == `{"type":"ping"}` {
			_ = sConn.WriteMessage(websocket.TextMessage, []byte(`{"type":"pong"}`))
			continue
		}

		if role == "sender" {
			// Nếu là tin nhắn TextMessage (Metadata JSON...):
			// Gửi trực tiếp cho Receiver ngay lập tức, không chiếm slot của chunk trong Queue
			if msgType == websocket.TextMessage {
				room.Lock.Lock()
				rcv := room.Receiver
				room.Lock.Unlock()
				if rcv != nil {
					_ = rcv.WriteMessage(msgType, data)
				}
			} else {
				// Kiểm tra xem Receiver có đang trong phòng không
				room.Lock.Lock()
				rcv := room.Receiver
				room.Lock.Unlock()

				if rcv == nil {
					_ = sConn.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"receiver"}`))
					break
				}

				// Binary chunk dữ liệu -> Đưa vào Queue (tối đa MaxChunksInRam = 32)
				// Đặt timeout 20s để không bao giờ bị Deadlock treo cứng nếu Receiver ngừng đọc TCP
				select {
				case room.Queue <- RelayMessage{MsgType: msgType, Data: data}:
				case <-time.After(20 * time.Second):
					log.Printf("[%s] Queue đầy quá 20s (Receiver nghẽn). Báo ngắt kết nối cho Sender\n", roomID)
					_ = sConn.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"receiver"}`))
					break
				}
			}
		} else {
			// BÊN NHẬN -> BÊN GỬI (Tín hiệu điều khiển: READY, VERIFY_OK, VERIFY_FAIL...)
			room.Lock.Lock()
			sender := room.Sender
			room.Lock.Unlock()
			if sender != nil {
				if err := sender.WriteMessage(msgType, data); err != nil {
					log.Printf("[%s] Lỗi gửi tin tới Sender: %v\n", roomID, err)
				}
			}
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/speedtest", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 2*1024*1024) // 2MB
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(buf)
	})

	http.HandleFunc("/ws", handleWS)

	log.Printf("v6drop Go Relay Server đang chạy tại cổng %s (Buffer RAM: tối đa %d chunks)\n", port, MaxChunksInRam)
	server := &http.Server{
		Addr: "0.0.0.0:" + port,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Lỗi server: %v\n", err)
	}
}
