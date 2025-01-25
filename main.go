package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
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
	WORKER_COUNT        = 10
)

var (
	websocketUrl = "ws://polygonproxyserver:8081/mock"

	polygonApiKey        = ""
	messageBuffer        = make(chan []byte, MESSAGE_BUFFER_SIZE)
	droppedMessages      int64
	kafkaProducer        sarama.AsyncProducer
	kafkaTopicTrades     = "trades"     // Kafka topic for tick-by-tick
	kafkaTopicAggregates = "aggregates" // Kafka topic for aggregated data

)

func init() {

	// Initialize Kafka (Asynchronous Producer)
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ChannelBufferSize = 10000

	// Enable asynchronous producer
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Return.Successes = false

	// Configure batching
	kafkaConfig.Producer.Flush.Messages = 500 // Adjust batch size
	kafkaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaConfig.Producer.Flush.Bytes = 1 * 1024 * 1024 // 1 MB batch size

	// Set retry behavior
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.Retry.Backoff = 100 * time.Millisecond

	// Required acks for reliability
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal

	// Compression for optimized network usage (optional, can use gzip/snappy)
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy

	// Enable metrics collection (useful for monitoring buffer usage)
	kafkaConfig.MetricRegistry = metrics.NewRegistry() // Integrate with Sarama metrics

	var err error
	kafkaBroker := []string{"kafka1:9092", "kafka2:9092"}
	kafkaProducer, err = sarama.NewAsyncProducer(kafkaBroker, kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka async producer: %v", err)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			registry := kafkaConfig.MetricRegistry
			if registry != nil {
				// Log average batch size
				if batchSizeMetric := registry.Get("producer-metrics/batch-size-avg"); batchSizeMetric != nil {
					log.Printf("[Metric] Average batch size: %v", batchSizeMetric)
				}

				// Log message send rate
				if sendRateMetric := registry.Get("producer-metrics/record-send-rate"); sendRateMetric != nil {
					log.Printf("[Metric] Message send rate: %v records/sec", sendRateMetric)
				}

				// Log pending messages in Kafka producer buffer
				pending := len(kafkaProducer.Input())
				log.Printf("[Metric] Pending messages in buffer: %d", pending)

				// Log Kafka producer retry rate
				if retryRateMetric := registry.Get("producer-metrics/retry-rate"); retryRateMetric != nil {
					log.Printf("[Metric] Retry rate: %v retries/sec", retryRateMetric)
				}
			} else {
				log.Println("[Metric] MetricRegistry is nil; skipping metrics collection")
			}
		}
	}()

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	log.Println("Kafka producer initialized successfully.")
}

func logDroppedMessages() {
	for {
		time.Sleep(1 * time.Minute)
		dropped := atomic.LoadInt64(&droppedMessages)
		if dropped > 0 {
			log.Printf("Dropped messages in the last minute: %d", dropped)
			atomic.StoreInt64(&droppedMessages, 0)
		}
	}
}

func isConnectedMessage(message []byte) bool {
	pattern := `"ev"\s*:\s*"status"\s*,\s*"status"\s*:\s*"connected"`
	re := regexp.MustCompile(pattern)
	return re.Match(message)
}

func sendAuthMessage(conn *websocket.Conn) error {
	msg := fmt.Sprintf(`{"action":"auth","params":"%v"}`, polygonApiKey)
	log.Println(msg)
	return conn.WriteMessage(websocket.TextMessage, []byte(msg))
}

func isAuthSuccessMessage(message []byte) bool {
	pattern := `"ev"\s*:\s*"status"\s*,\s*"status"\s*:\s*"auth_success"`
	re := regexp.MustCompile(pattern)
	return re.Match(message)
}

func sendTradeSubscribeMessage(conn *websocket.Conn) error {
	msg := `{"action":"subscribe", "params":"T.*,AM.*"}`
	log.Println("Subscribing to trades")
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

var producedMessages int64

//var messageBufferMutex sync.Mutex

func connectAndReceive() {
	log.Printf("Connecting to '%v'\n", websocketUrl)
	dialer := websocket.Dialer{
		ReadBufferSize:  8192,
		WriteBufferSize: 1024,
	}
	conn, _, err := dialer.Dial(websocketUrl, nil)
	if err != nil {
		log.Printf("dialer.Dial Error: %s", err)
		return
	}
	log.Println("Connected to " + websocketUrl)
	defer conn.Close()

	isSubscribed := false
	for {
		_, message, err := conn.ReadMessage()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			log.Println("Connection closed normally: " + err.Error())
			return
		}
		if websocket.IsCloseError(err, websocket.CloseServiceRestart) {
			log.Println("Connection closed (service restart): " + err.Error())
			return
		}
		if err != nil {
			log.Printf("WebSocket read error: %s", err)
			return
		}

		if isSubscribed {
			select {
			case messageBuffer <- message:
				// Increment the counter
				atomic.AddInt64(&producedMessages, 1)
			default:
				atomic.AddInt64(&droppedMessages, 1)
			}
		} else {
			isSubscribed = processStatusMessage(conn, message)
		}
	}
}

var consumedMessages int64

func processMessageWorker() {

	for message := range messageBuffer {
		atomic.AddInt64(&consumedMessages, 1)
		var events []map[string]interface{}
		if err := json.Unmarshal(message, &events); err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}

		for _, event := range events {
			switch event["ev"] {
			case "T": // Tick-by-tick trades
				sendToKafka(kafkaTopicTrades, event)
			case "AM": // Aggregated data
				sendToKafka(kafkaTopicAggregates, event)
			default:
				log.Printf("Unknown event type: %v", event["ev"])
			}
		}
	}
}

const maxRetries = 5

var receivedMessages int64

func logMessagesPerSecond() {
	for {
		time.Sleep(1 * time.Second)
		count := atomic.SwapInt64(&receivedMessages, 0)
		log.Printf("Messages received per second: %d", count)
	}
}

var producerMutex sync.Mutex

func sendToKafka(topic string, event map[string]interface{}) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[Error] Failed to marshal event: %v", err)
		return
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	producerMutex.Lock()
	defer producerMutex.Unlock()

	select {
	case kafkaProducer.Input() <- message:
		log.Printf("[Info] Message sent to Kafka. Topic: %s, Size: %d bytes", topic, len(data))
	case err := <-kafkaProducer.Errors():
		log.Printf("[Error] Kafka producer error: %v", err)
	default:
		log.Printf("[Default Case] Kafka buffer unavailable. Topic: %s, Size: %d bytes", topic, len(data))
		// Print the full event in JSON format
		fullEvent, err := json.MarshalIndent(event, "", "  ")
		if err != nil {
			log.Printf("[Error] Failed to marshal full event for logging: %v", err)
		} else {
			log.Printf("[Default Case] Full Event: %s", fullEvent)
		}
	}
}

/*
func sendToKafka(topic string, event map[string]interface{}) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal event for Kafka: %v", err)
		return
	}

	kafkaProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
}
*/

func monitorRates() {
	for {
		time.Sleep(1 * time.Second)
		produced := atomic.SwapInt64(&producedMessages, 0)
		consumed := atomic.SwapInt64(&consumedMessages, 0)
		log.Printf("Messages produced/consumed per sec : %d/s, consumed: %d/s", produced, consumed)
		//log.Printf("Buffer utilization: %d/%d", len(kafkaProducer.Input()), cap(kafkaProducer.Input()))

	}
}

func monitorRates30Sec() {
	for {
		time.Sleep(30 * time.Second)
		produced := atomic.SwapInt64(&producedMessages, 0)
		consumed := atomic.SwapInt64(&consumedMessages, 0)
		log.Printf("Avg Messages produced/consumed per sec in last 30 sec: %d/s, consumed: %f/s", float64(produced)/float64(30), float64(consumed)/float64(30))
		//log.Printf("Buffer utilization: %d/%d", len(kafkaProducer.Input()), cap(kafkaProducer.Input()))

	}
}

const kafkaBufferCapacity = 10000

func monitorKafkaBuffer() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		currentBufferSize := len(kafkaProducer.Input())
		remainingBuffer := kafkaBufferCapacity - currentBufferSize

		log.Printf("Kafka buffer usage: %d/%d (remaining: %d)", currentBufferSize, kafkaBufferCapacity, remainingBuffer)

		if currentBufferSize >= kafkaBufferCapacity {
			log.Println("Warning: Kafka producer buffer is full!")
		}
	}
}

func main() {
	//go logMessagesPerSecond()
	go monitorRates()
	go logDroppedMessages()
	go monitorKafkaBuffer()
	//go monitorRates30Sec()

	// Start worker pool
	for i := 0; i < WORKER_COUNT; i++ {
		go processMessageWorker()
	}

	for {
		connectAndReceive()
		log.Println("Reconnecting to WebSocket server...")
		time.Sleep(RECONNECT_DELAY)
	}
}
