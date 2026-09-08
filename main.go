package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256 * 1024,
	WriteBufferSize: 256 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Room struct {
	Sender   *websocket.Conn
	Receiver *websocket.Conn
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
	}

	// Nếu cả 2 đều đã rời phòng, dọn dẹp sạch Room để giải phóng RAM
	if room.Sender == nil && room.Receiver == nil {
		delete(rooms, roomID)
		log.Printf("[%s] Phòng đã rỗng và được giải phóng khỏi RAM\n", roomID)
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

		// Forward dữ liệu: Sender -> Receiver hoặc ACK từ Receiver -> Sender
		room.Lock.Lock()
		var target *websocket.Conn
		if role == "sender" {
			target = room.Receiver
		} else {
			target = room.Sender
		}

		if target != nil {
			_ = target.WriteMessage(msgType, data)
		}
		room.Lock.Unlock()
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("v6drop Go relay server is running healthy!"))
	})

	http.HandleFunc("/ws", handleWS)

	// Phục vụ frontend static files trong thư mục ./public
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	log.Printf("v6drop Go Relay Server đang chạy tại cổng %s\n", port)
	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Lỗi server: %v\n", err)
	}
}
