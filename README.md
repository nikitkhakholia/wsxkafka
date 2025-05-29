
# 📘 Real-Time Market Data Pipeline (Node.js + Kafka + WebSocket)

This project demonstrates a real-time data pipeline built with Node.js, simulating a live crypto trading data flow. It connects to the Kraken WebSocket API, ingests real-time ticker data (e.g., BTC/USD, ETH/USD), processes it, and streams updates to WebSocket clients using a locally hosted Kafka message broker.


## Features

- ✅ Connects to Kraken WebSocket API (```wss://ws.kraken.com```)
- ✅ Subscribes to ticker updates for multiple crypto pairs (e.g., BTC/USD, ETH/USD)
- ✅ Extracts key data: symbol, timestamp, bid, ask
- ✅ Publishes each update as a JSON message to a local Kafka topic (quotes.crypto)
- ✅ Kafka consumer reads the topic in real time
- ✅ WebSocket server broadcasts the updates to all connected clients (```ws://localhost:8080```)
- ✅ Handles client connections, disconnections, and errors gracefully

---
## ✅ Setup and Execution Steps
- Ensure Java 17 or higher is installed on your system(to run Apache Kafka).
- Download and install Apache Kafka from: ```https://kafka.apache.org/documentation/#quickstart```.
- Run ```npm install``` to install the required packages (e.g., kafkajs, ws)
- Start ZooKeeper and Kafka; verify Kafka is running at ```localhost:9092```
- Run ```wsconsumer_kafkaproducer.js``` to connect to the ```Kraken WebSocket API``` and publish real-time ticker data (BTC/USD, ETH/USD) to the Kafka topic ```quotes.crypto```
- Run ```kafkaconsumer_wss.js``` to consume data from the Kafka topic and broadcast it via a WebSocket server.
- Connect any WebSocket client (e.g., Postman) to ```ws://localhost:8080``` to receive the real-time data stream.
