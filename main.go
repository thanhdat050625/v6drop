package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// SafeConn bọc websocket.Conn với sync.Mutex để chống xung đột luồng khi WriteMessage (Gorilla WS quy định không được write đồng thời)
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
	return s.conn.WriteMessage(messageType, data)
}

func (s *SafeConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

type Room struct {
	Sender   *SafeConn
	Receiver *SafeConn
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
	r := &Room{}
	rooms[id] = r
	return r
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
	}

	if room.Sender == nil && room.Receiver == nil {
		delete(rooms, roomID)
		log.Printf("[%s] Phòng đã rỗng -> Giải phóng bộ nhớ hoàn toàn\n", roomID)
	}
	room.Lock.Unlock()
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Lỗi WS Upgrade: %v\n", err)
		return
	}

	// Đảm bảo không giới hạn đọc
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
	}
	room.Lock.Unlock()

	defer removePeerFromRoom(roomID, role, sConn)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Xử lý ping giữ kết nối
		if msgType == websocket.TextMessage && string(data) == `{"type":"ping"}` {
			_ = sConn.WriteMessage(websocket.TextMessage, []byte(`{"type":"pong"}`))
			continue
		}

		if role == "sender" {
			// BÊN GỬI -> BÊN NHẬN (chuyển tiếp trực tiếp Metadata và Chunks)
			room.Lock.Lock()
			receiver := room.Receiver
			room.Lock.Unlock()
			if receiver != nil {
				if err := receiver.WriteMessage(msgType, data); err != nil {
					log.Printf("[%s] Lỗi gửi tới Receiver: %v\n", roomID, err)
				}
			}
		} else {
			// BÊN NHẬN -> BÊN GỬI (chuyển tiếp tín hiệu ACK)
			room.Lock.Lock()
			sender := room.Sender
			room.Lock.Unlock()
			if sender != nil {
				if err := sender.WriteMessage(msgType, data); err != nil {
					log.Printf("[%s] Lỗi gửi tới Sender: %v\n", roomID, err)
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

	http.HandleFunc("/ws", handleWS)

	log.Printf("v6drop Go Relay Server đang chạy tại cổng %s\n", port)
	server := &http.Server{
		Addr: "0.0.0.0:" + port,
		// Lưu ý: Không đặt ReadTimeout và WriteTimeout trên http.Server vì sẽ làm đứt kết nối WebSocket đang streaming
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Lỗi server: %v\n", err)
	}
}
