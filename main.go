package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Giới hạn tối đa đúng 10 chunk trong RAM (10 * 256KB = 2.5MB RAM)
// Nếu bên gửi đẩy lên quá nhanh mà bên nhận chưa kịp đọc, server sẽ tự động hãm tốc độ bên gửi (Backpressure)
const MaxChunksInRam = 10

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256 * 1024,
	WriteBufferSize: 256 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type RelayMessage struct {
	MsgType int
	Data    []byte
}

type Room struct {
	Sender    *websocket.Conn
	Receiver  *websocket.Conn
	Queue     chan RelayMessage // Channel buffer giới hạn đúng 10 chunk trong RAM
	StopChan  chan struct{}
	Lock      sync.Mutex
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
		Queue:    make(chan RelayMessage, MaxChunksInRam),
		StopChan: make(chan struct{}),
	}
	rooms[id] = r
	return r
}

// Xả sạch hàng đợi để giải phóng RAM ngay lập tức khi ngắt kết nối
func drainQueue(ch chan RelayMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func removePeerFromRoom(roomID, role string, conn *websocket.Conn) {
	roomsLock.Lock()
	defer roomsLock.Unlock()

	room, exists := rooms[roomID]
	if !exists {
		return
	}

	room.Lock.Lock()
	if role == "sender" && room.Sender == conn {
		room.Sender = nil
		log.Printf("[%s] Sender ngắt kết nối\n", roomID)
		if room.Receiver != nil {
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"sender"}`))
		}
	} else if role == "receiver" && room.Receiver == conn {
		room.Receiver = nil
		log.Printf("[%s] Receiver ngắt kết nối\n", roomID)
		if room.Sender != nil {
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_DISCONNECTED","role":"receiver"}`))
		}
		// Xả sạch RAM nếu Receiver ngắt kết nối
		drainQueue(room.Queue)
	}

	// Nếu cả 2 đều đã rời phòng, dọn dẹp sạch Room để giải phóng RAM
	if room.Sender == nil && room.Receiver == nil {
		select {
		case <-room.StopChan:
		default:
			close(room.StopChan)
		}
		drainQueue(room.Queue)
		delete(rooms, roomID)
		log.Printf("[%s] Phòng đã rỗng và được giải phóng khỏi RAM (Zero Memory Leak)\n", roomID)
	}
	room.Lock.Unlock()
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Lỗi WS Upgrade: %v\n", err)
		return
	}
	defer conn.Close()

	// Cho phép nhận chunk 256KB thoải mái
	conn.SetReadLimit(10 * 1024 * 1024)

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
		room.Sender = conn
		log.Printf("[%s] Bên Gửi (Sender) đã kết nối\n", roomID)
		if room.Receiver != nil {
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"sender"}`))
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"receiver"}`))
		}
	} else {
		if room.Receiver != nil {
			_ = room.Receiver.Close()
		}
		room.Receiver = conn
		log.Printf("[%s] Bên Nhận (Receiver) đã kết nối\n", roomID)
		if room.Sender != nil {
			_ = room.Sender.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"receiver"}`))
			_ = room.Receiver.WriteMessage(websocket.TextMessage, []byte(`{"type":"PEER_CONNECTED","role":"sender"}`))
		}

		// Khởi chạy goroutine chuyên trách đọc từ Queue (tối đa 10 chunk) và gửi ra Receiver
		go func(rcvConn *websocket.Conn, q chan RelayMessage, stop chan struct{}) {
			for {
				select {
				case msg, ok := <-q:
					if !ok {
						return
					}
					if err := rcvConn.WriteMessage(msg.MsgType, msg.Data); err != nil {
						return
					}
				case <-stop:
					return
				}
			}
		}(conn, room.Queue, room.StopChan)
	}
	room.Lock.Unlock()

	defer removePeerFromRoom(roomID, role, conn)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Xử lý ping giữ kết nối chống ngủ đông trên Render Free
		if msgType == websocket.TextMessage && string(data) == `{"type":"ping"}` {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"pong"}`))
			continue
		}

		if role == "sender" {
			// BÊN GỬI -> BÊN NHẬN:
			// Đưa vào Channel Queue (có sức chứa tối đa đúng 10 chunk).
			// Nếu trong RAM đã có 10 chunk chưa kịp gửi tới Receiver, dòng này sẽ TỰ ĐỘNG CHỜ (BLOCK),
			// ép socket TCP của bên gửi dừng đọc -> TCP Flow Control ép trình duyệt bên gửi hãm tốc độ lại.
			select {
			case room.Queue <- RelayMessage{MsgType: msgType, Data: data}:
			case <-room.StopChan:
				return
			}
		} else {
			// BÊN NHẬN -> BÊN GỬI (Tín hiệu ACK phản hồi flow control):
			// Tín hiệu ACK cực nhẹ (< 10 bytes) được chuyển thẳng trực tiếp về Sender, không qua queue 10 chunk
			room.Lock.Lock()
			sender := room.Sender
			room.Lock.Unlock()
			if sender != nil {
				_ = sender.WriteMessage(msgType, data)
			}
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("v6drop Go relay server is running healthy!"))
	})

	http.HandleFunc("/ws", handleWS)

	// Phục vụ frontend static files trong thư mục ./public
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	log.Printf("v6drop Go Relay Server đang chạy tại cổng %s (Buffer RAM: tối đa %d chunks)\n", port, MaxChunksInRam)
	server := &http.Server{
		Addr:         "0.0.0.0:" + port,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Lỗi server: %v\n", err)
	}
}
