package main

// import (
// 	"bufio"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"net"
// 	"net/http"
// 	"os"
// 	"sync"
// 	"time"

// 	"github.com/gorilla/websocket"
// )

// var upgrader = websocket.Upgrader{
// 	CheckOrigin: func(r *http.Request) bool {
// 		return true // Allow all origins for testing
// 	},
// }

// // Data storage for file content
// var fileData []string
// var fileDataMutex sync.RWMutex

// // Status message to notify connection success
// var statusMessage = `{"ev":"status","status":"connected"}`

// // Subscribe and Auth message structure
// type ClientMessage struct {
// 	Action string `json:"action"`
// 	Params string `json:"params"`
// }

// // Track active clients
// var activeClients sync.Map

// // Load data from a file
// func loadDataFromFile(filePath string) {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		log.Fatalf("Failed to open file: %v", err)
// 	}
// 	defer file.Close()

// 	// Read the file line by line
// 	scanner := bufio.NewScanner(file)
// 	var lines []string
// 	for scanner.Scan() {
// 		lines = append(lines, scanner.Text())
// 	}

// 	if err := scanner.Err(); err != nil {
// 		log.Fatalf("Failed to read file: %v", err)
// 	}

// 	// Store the data in memory
// 	fileDataMutex.Lock()
// 	fileData = lines
// 	fileDataMutex.Unlock()
// 	log.Printf("Loaded %d lines from file: %s", len(lines), filePath)
// }

// // WebSocket handler with concurrent client support
// func mockWebSocketHandler(messagesPerSecond int, randomize bool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		// Upgrade the HTTP connection to a WebSocket connection
// 		conn, err := upgrader.Upgrade(w, r, nil)
// 		if err != nil {
// 			log.Println("Upgrade error:", err)
// 			return
// 		}

// 		// Determine client IP
// 		clientIP := r.Header.Get("X-Forwarded-For")
// 		if clientIP == "" {
// 			clientIP, _, err = net.SplitHostPort(r.RemoteAddr)
// 			if err != nil {
// 				log.Printf("Error parsing client address: %v", err)
// 				clientIP = "unknown"
// 			}
// 		}
// 		log.Printf("New client connected from IP: %s, Message playback rate: %d messages/second", clientIP, messagesPerSecond)

// 		// Handle the client connection in a separate goroutine
// 		go handleClientConnection(conn, messagesPerSecond, randomize, clientIP)
// 	}
// }

// // Handle an individual client connection
// func handleClientConnection(conn *websocket.Conn, messagesPerSecond int, randomize bool, clientIP string) {
// 	clientID := time.Now().UnixNano() // Unique identifier for each client
// 	activeClients.Store(clientID, conn)
// 	defer func() {
// 		activeClients.Delete(clientID)
// 		conn.Close()
// 		log.Printf("Client %d (IP: %s) disconnected", clientID, clientIP)
// 	}()

// 	log.Printf("Client %d (IP: %s) connected", clientID, clientIP)

// 	// Send status message
// 	if err := conn.WriteMessage(websocket.TextMessage, []byte(statusMessage)); err != nil {
// 		log.Printf("Failed to send status message to client %d (IP: %s): %v", clientID, clientIP, err)
// 		return
// 	}
// 	log.Printf("Sent status message to client %d (IP: %s)", clientID, clientIP)

// 	for {
// 		// Read message from client
// 		_, msg, err := conn.ReadMessage()
// 		if err != nil {
// 			log.Printf("Client %d (IP: %s) disconnected: %v", clientID, clientIP, err)
// 			break
// 		}

// 		// Debug log for raw message
// 		log.Printf("Raw message received from client %d (IP: %s): %s", clientID, clientIP, string(msg))

// 		// Parse the client message
// 		var clientMsg ClientMessage
// 		if err := json.Unmarshal(msg, &clientMsg); err != nil {
// 			log.Printf("Invalid message from client %d (IP: %s): %v", clientID, clientIP, err)
// 			continue
// 		}

// 		// Debug log for parsed values
// 		log.Printf("Parsed client message from client %d (IP: %s): Action=%s, Params=%s", clientID, clientIP, clientMsg.Action, clientMsg.Params)

// 		// Handle the action
// 		switch clientMsg.Action {
// 		case "auth":
// 			log.Printf("Client %d (IP: %s) authenticated with params: %s", clientID, clientIP, clientMsg.Params)
// 			authSuccessMessage := `{"ev":"status","status":"auth_success"}`
// 			conn.WriteMessage(websocket.TextMessage, []byte(authSuccessMessage))
// 		case "subscribe":
// 			log.Printf("Client %d (IP: %s) subscribed with params: %s", clientID, clientIP, clientMsg.Params)
// 			// Start sending file data to this client
// 			sendFileData(conn, messagesPerSecond, randomize, clientID, clientIP)
// 			return
// 		default:
// 			log.Printf("Unknown action from client %d (IP: %s): %s", clientID, clientIP, clientMsg.Action)
// 		}
// 	}
// }

// // Send data from the loaded file to a single client
// func sendFileData(conn *websocket.Conn, messagesPerSecond int, randomize bool, clientID int64, clientIP string) {
// 	ticker := time.NewTicker(time.Second / time.Duration(messagesPerSecond))
// 	defer ticker.Stop()

// 	fileDataMutex.RLock()
// 	defer fileDataMutex.RUnlock()

// 	dataIndex := 0
// 	for {
// 		select {
// 		case <-ticker.C:
// 			if len(fileData) == 0 {
// 				log.Printf("No data available to send to client %d (IP: %s)", clientID, clientIP)
// 				return
// 			}

// 			var message string
// 			if randomize {
// 				message = fileData[rand.Intn(len(fileData))]
// 			} else {
// 				message = fileData[dataIndex]
// 				dataIndex = (dataIndex + 1) % len(fileData)
// 			}

// 			if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
// 				log.Printf("Write error or client %d (IP: %s) disconnected: %v", clientID, clientIP, err)
// 				return
// 			}
// 		}
// 	}
// }

// func main() {
// 	// Command-line arguments
// 	filePath := flag.String("file", "data.txt", "Path to the data file")
// 	messagesPerSecond := flag.Int("rate", 5, "Number of messages to send per second")
// 	randomize := flag.Bool("randomize", false, "Send messages in random order")
// 	port := flag.Int("port", 8081, "Port number to run the WebSocket server")
// 	flag.Parse()

// 	// Load data from the specified file
// 	loadDataFromFile(*filePath)

// 	// Log active clients periodically
// 	go func() {
// 		for range time.Tick(60 * time.Second) {
// 			clientCount := 0
// 			activeClients.Range(func(_, _ interface{}) bool {
// 				clientCount++
// 				return true
// 			})
// 			log.Printf("Currently active clients: %d", clientCount)
// 		}
// 	}()

// 	http.HandleFunc("/mock", func(w http.ResponseWriter, r *http.Request) {
// 		mockWebSocketHandler(*messagesPerSecond, *randomize)(w, r)
// 	})

// 	address := fmt.Sprintf("0.0.0.0:%d", *port) // Bind to IPv4 only
// 	log.Printf("Starting WebSocket server on ws://%s/mock (file: %s)", address, *filePath)
// 	log.Fatal(http.ListenAndServe(address, nil))
// }
