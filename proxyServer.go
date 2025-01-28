package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	activeClients sync.Map
	messageBuffer = make(chan []byte, 10000)
)

func connectToPolygon(apiKey, wsURL string) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Failed to connect to Polygon: %v, retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		// Authenticate
		authMsg := fmt.Sprintf(`{"action":"auth","params":"%s"}`, apiKey)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(authMsg)); err != nil {
			log.Printf("Authentication failed: %v", err)
			continue
		}

		// Subscribe to trades and aggregates
		subscribeMsg := `{"action":"subscribe","params":"T.*,AM.*"}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
			log.Printf("Subscription failed: %v", err)
			continue
		}

		log.Println("Connected to Polygon WebSocket API")

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Read error from Polygon: %v", err)
				break
			}

			select {
			case messageBuffer <- message:
			default:
				log.Println("Message buffer full, dropping message")
			}
		}
	}
}

func handleClientConnection(conn *websocket.Conn, clientIP string) {
	clientID := time.Now().UnixNano()
	activeClients.Store(clientID, conn)
	defer func() {
		activeClients.Delete(clientID)
		conn.Close()
		log.Printf("Client %d (IP: %s) disconnected", clientID, clientIP)
	}()

	// Send initial status message
	statusMsg := `{"ev":"status","status":"connected"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(statusMsg)); err != nil {
		log.Printf("Failed to send status message: %v", err)
		return
	}

	// Handle client messages
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var clientMsg struct {
			Action string `json:"action"`
			Params string `json:"params"`
		}
		if err := json.Unmarshal(msg, &clientMsg); err != nil {
			continue
		}

		switch clientMsg.Action {
		case "auth":
			authSuccessMsg := `{"ev":"status","status":"auth_success"}`
			conn.WriteMessage(websocket.TextMessage, []byte(authSuccessMsg))
		case "subscribe":
			// Client is now ready to receive messages
			return
		}
	}
}

func broadcastMessages() {
	for msg := range messageBuffer {
		activeClients.Range(func(_, value interface{}) bool {
			if conn, ok := value.(*websocket.Conn); ok {
				conn.WriteMessage(websocket.TextMessage, msg)
			}
			return true
		})
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	apiKey := os.Getenv("POLYGON_API_KEY")
	wsURL := os.Getenv("POLYGON_WS_URL")
	port := os.Getenv("PORT")

	if apiKey == "" || wsURL == "" {
		log.Fatal("POLYGON_API_KEY and POLYGON_WS_URL are required")
	}

	// Connect to Polygon WebSocket
	go connectToPolygon(apiKey, wsURL)

	// Start broadcasting messages to clients
	go broadcastMessages()

	// WebSocket endpoint
	http.HandleFunc("/mock", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Upgrade error:", err)
			return
		}
		go handleClientConnection(conn, r.RemoteAddr)
	})

	address := fmt.Sprintf("0.0.0.0:%s", port)
	log.Printf("Starting WebSocket server on ws://%s/mock", address)
	log.Fatal(http.ListenAndServe(address, nil))
}