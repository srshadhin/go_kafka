# High-Throughput WebSocket-to-Kafka Pipeline

This project is a high-throughput pipeline that consumes messages from a WebSocket server, processes them, and writes them to Apache Kafka. It is designed to handle **25k–50k messages per second** with minimal data loss and high reliability.

## Features

- **WebSocket Integration**: Connects to a WebSocket server to receive real-time messages.
- **Kafka Producer**: Writes messages to Kafka topics with batching, compression, and retries.
- **High Throughput**: Optimized for handling 25k–50k messages per second.
- **Non-Blocking Design**: Uses buffered channels and worker pools to ensure non-blocking message processing.
- **Metrics and Monitoring**: Logs metrics such as message throughput, buffer utilization, and dropped messages.
- **Configurable**: Easily configurable Kafka and WebSocket settings.

## Prerequisites

- **Go 1.20+**: Install Go from [here](https://golang.org/dl/).
- **Docker**: Install Docker from [here](https://docs.docker.com/get-docker/).
- **Apache Kafka**: Run Kafka using the provided Docker Compose file.

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/srshadin/go_kafka.git
cd go_kafka
```
### 2. Start Kafka and Zookeeper

Use the provided  `docker-compose.yml`  file to start Kafka and Zookeeper:

```bash
docker-compose up
```
This will start:

-   Zookeeper on port  `2181`

-   Kafka on port  `9092`


### 3. Configure the Project

Update the following environment variables in the  `.env`  file or set them in your shell:

```bash
export POLYGON_API_KEY="your_polygon_api_key"
export WEBSOCKET_URL="ws://0.0.0.0:8081/mock"
export KAFKA_BROKERS="localhost:9092"
```
### 4. Build and Run the Application

Build and run the Go application:

1. Run Proxy Server:
    ```bash
    go run polygonProxyServer.go
    ```
2. Run Main Application:
    ```bash
    go build && go run main.go 
    ``` 
### 5. Monitor Metrics

The application logs metrics every 30 seconds, including:

-   Messages received, produced, and consumed per second.

-   Kafka buffer utilization.

-   Dropped messages.


Example log output:
[Metrics] Received: 25000, Produced: 25000, Consumed: 25000, Dropped: 0, Buffer: 0/1000000

----------

### Kafka Topics

The application writes messages to the following Kafka topics:

-   **trades**: For tick-by-tick trade data.

-   **aggregates**: For aggregated data.


You can modify the topics in the  `main.go`  file.

----------

## Architecture

### Components

1.  **WebSocket Client**:

    -   Connects to the WebSocket server.

    -   Authenticates and subscribes to trade and aggregate data.

    -   Buffers incoming messages in a channel.

2.  **Worker Pool**:

    -   Processes messages from the buffer concurrently.

    -   Routes messages to the appropriate Kafka topic.

3.  **Kafka Producer**:

    -   Writes messages to Kafka with batching, compression, and retries.

    -   Handles backpressure and logs errors.

4.  **Metrics and Monitoring**:

    -   Logs throughput, buffer utilization, and dropped messages.

    -   Provides insights into system performance.


----------

## Performance Optimization

### Kafka Producer Settings

The Kafka producer is configured for high throughput:

-   **Batching**: Messages are batched to reduce network overhead.

-   **Compression**: Messages are compressed using Snappy.

-   **Retries**: Failed messages are retried up to 5 times.


### Worker Pool

A pool of workers processes messages concurrently to avoid bottlenecks. The number of workers can be adjusted in the  `WORKER_COUNT`  constant.

### Buffered Channels

Buffered channels are used to decouple message reception, processing, and Kafka production. This ensures non-blocking behavior and high throughput.

----------

## Troubleshooting

### Common Issues

1.  **Kafka Broker Unreachable**:

    -   Ensure Kafka is running and accessible.

    -   Check the Kafka logs for errors:

        ```bash
        docker-compose logs kafka
        ```

2.  **Buffer Full**:

    -   Increase the buffer size (`KAFKA_BUFFER_SIZE`) if messages are being dropped.

    -   Optimize Kafka producer settings for higher throughput.

3.  **WebSocket Connection Issues**:

    -   Verify the WebSocket server URL and API key.

    -   Check the WebSocket server logs for errors.


----------

## Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.

2.  Create a new branch for your feature or bugfix.

3.  Submit a pull request.


----------

## License

This project is licensed under the MIT License. See the  [LICENSE](https://chat.deepseek.com/a/chat/s/LICENSE)  file for details.

----------

## Acknowledgments

-   [Sarama](https://github.com/IBM/sarama): Kafka client library for Go.

-   [Gorilla WebSocket](https://github.com/gorilla/websocket): WebSocket library for Go.

-   [Docker](https://www.docker.com/): Containerization platform.

---