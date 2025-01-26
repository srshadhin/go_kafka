package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"github.com/rcrowley/go-metrics"
)

const (
	INACTIVITY_TIMEOUT  = 1 * time.Second
	RECONNECT_DELAY     = 1 * time.Minute
	MESSAGE_BUFFER_SIZE = 100000
	WORKER_COUNT        = 20 // Increase worker count for higher throughput
	KAFKA_BUFFER_SIZE   = 100000
)

var (
	websocketUrl  = "ws://0.0.0.0:8081/mock"
	polygonApiKey = ""

	messageBuffer        = make(chan []byte, MESSAGE_BUFFER_SIZE)                // Buffered channel for WebSocket messages
	kafkaMessageBuffer   = make(chan *sarama.ProducerMessage, KAFKA_BUFFER_SIZE) // Buffered channel for Kafka messages
	droppedMessages      int64
	kafkaProducer        sarama.AsyncProducer
	kafkaTopicTrades     = "trades"
	kafkaTopicAggregates = "aggregates"

	producedMessages int64
	consumedMessages int64
	receivedMessages int64
)

func init() {
	// Initialize Kafka producer
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Return.Successes = false
	kafkaConfig.Producer.Flush.Messages = 1000
	kafkaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.MetricRegistry = metrics.NewRegistry()

	var err error
	kafkaBroker := []string{"localhost:9092"}
	kafkaProducer, err = sarama.NewAsyncProducer(kafkaBroker, kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka async producer: %v", err)
	}

	// Start Kafka message dispatcher
	go func() {
		for msg := range kafkaMessageBuffer {
			kafkaProducer.Input() <- msg
		}
	}()

	// Start metrics logging
	go logMetrics()
}

func logMetrics() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		produced := atomic.LoadInt64(&producedMessages)
		consumed := atomic.LoadInt64(&consumedMessages)
		received := atomic.LoadInt64(&receivedMessages)
		dropped := atomic.LoadInt64(&droppedMessages)
		bufferUtilization := len(kafkaMessageBuffer)

		log.Printf("[Metrics] Received: %d, Produced: %d, Consumed: %d, Dropped: %d, Buffer: %d/%d",
			received, produced, consumed, dropped, bufferUtilization, KAFKA_BUFFER_SIZE)
	}
}

func isConnectedMessage(message []byte) bool {
	pattern := `"ev"\s*:\s*"status"\s*,\s*"status"\s*:\s*"connected"`
	re := regexp.MustCompile(pattern)
	return re.Match(message)
}

func sendAuthMessage(conn *websocket.Conn) error {
	msg := fmt.Sprintf(`{"action":"auth","params":"%v"}`, polygonApiKey)
	return conn.WriteMessage(websocket.TextMessage, []byte(msg))
}

func isAuthSuccessMessage(message []byte) bool {
	pattern := `"ev"\s*:\s*"status"\s*,\s*"status"\s*:\s*"auth_success"`
	re := regexp.MustCompile(pattern)
	return re.Match(message)
}

func sendTradeSubscribeMessage(conn *websocket.Conn) error {
	msg := `{"action":"subscribe", "params":"T.*,AM.*"}`
	return conn.WriteMessage(websocket.TextMessage, []byte(msg))
}

func processStatusMessage(conn *websocket.Conn, message []byte) bool {
	if isConnectedMessage(message) {
		if err := sendAuthMessage(conn); err != nil {
			log.Printf("sendAuthMessage Error: %s", err)
		}
	}
	if isAuthSuccessMessage(message) {
		if err := sendTradeSubscribeMessage(conn); err != nil {
			log.Printf("sendTradeSubscribeMessage Error: %s", err)
		}
		return true
	}
	return false
}

func connectAndReceive() {
	dialer := websocket.Dialer{
		ReadBufferSize:  8192,
		WriteBufferSize: 1024,
	}
	conn, _, err := dialer.Dial(websocketUrl, nil)
	if err != nil {
		log.Printf("dialer.Dial Error: %s", err)
		return
	}
	defer conn.Close()

	isSubscribed := false
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket read error: %s", err)
			return
		}

		if isSubscribed {
			select {
			case messageBuffer <- message:
				atomic.AddInt64(&receivedMessages, 1)
			default:
				atomic.AddInt64(&droppedMessages, 1)
			}
		} else {
			isSubscribed = processStatusMessage(conn, message)
		}
	}
}

func processMessageWorker() {
	for message := range messageBuffer {
		var events []map[string]interface{}
		if err := json.Unmarshal(message, &events); err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}

		for _, event := range events {
			switch event["ev"] {
			case "T":
				sendToKafka(kafkaTopicTrades, event)
			case "AM":
				sendToKafka(kafkaTopicAggregates, event)
			default:
				log.Printf("Unknown event type: %v", event["ev"])
			}
		}
		atomic.AddInt64(&consumedMessages, 1)
	}
}

func sendToKafka(topic string, event map[string]interface{}) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case kafkaMessageBuffer <- msg:
		atomic.AddInt64(&producedMessages, 1)
	default:
		atomic.AddInt64(&droppedMessages, 1)
		log.Printf("Kafka buffer full, dropped message for topic: %s", topic)
	}
}

func main() {
	// Start worker pool
	for i := 0; i < WORKER_COUNT; i++ {
		go processMessageWorker()
	}

	// Start WebSocket connection
	for {
		connectAndReceive()
		log.Println("Reconnecting to WebSocket server...")
		time.Sleep(RECONNECT_DELAY)
	}
}
