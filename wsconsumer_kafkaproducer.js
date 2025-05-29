const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

// Kraken WebSocket v2 public URL
const KRAKEN_WS_URL = 'wss://ws.kraken.com/v2';

//kafka broker instance at local
const kafka = new Kafka({ clientId: 'kraken-producer', brokers: ['localhost:9092'] });
// #kafka producer
const producer = kafka.producer();


// Create the WebSocket connection
const ws = new WebSocket(KRAKEN_WS_URL);

// Subscribe message for ticker
const subscribeMsg = {
    method: "subscribe",
    params: {
        channel: "ticker",
        symbol: ["BTC/USD", "ETH/USD"] // You can add more symbols here
    },
};

//successfull websocket connection
ws.on('open', async () => {
    try {
        console.log('🟢 Connected to Kraken WebSocket API');

        // connecting kafka producer only on successfull connection
        await producer.connect()
        console.log('🟢 Connected to Local Kafka Broker');

        // subscribing to Kraken Ticker data only if Kafka is up
        ws.send(JSON.stringify(subscribeMsg));

    } catch (err) {
        console.error("🔴 error after connecting to websocket", err.message)
    }
});
const productDataIntoKafkaAfterFormatting = (data) => {
    try {
        // required formatting in data
        var dataFormatted = {
            symbol:data.symbol.split("/").join(""),
            timestamp: new Date().toISOString(),
            bid: data.bid,
            ask: data.ask
        }
        console.log('⭐️ Karen Ticker Update', JSON.stringify(dataFormatted));
        
        // producing formatted data into kafka
        producer.send(
            {
                topic: "quotes.crypto",
                messages: [{ value: JSON.stringify(dataFormatted) }]
            }
        )
    } catch (err) { throw err }
}

// Handle incoming messages
ws.on('message', (data) => {
    try {
        const message = JSON.parse(data);
        // hamdling and logging socket messages
        if (message.channel == "ticker" && (message.type == "snapshot" || message.type == "update")) productDataIntoKafkaAfterFormatting(message.data[0])
        else if (message.channel == "status") console.table(message.data)
        else if (message.method == "subscribe") console.log("💡 Subscribed: ", message.result.symbol);
        else if (message.channel == "heartbeat") console.log("🤍");
        
        else {
            console.log("🔴 Message Not Handeled", message);
        }
    } catch (err) {
        console.error('🔴 Error parsing message:', err);
    }
});

// Handle errors
ws.on('error', (err) => {
    console.error('🔴 WebSocket error:', err);
});

// Handle connection close
ws.on('close', () => {
    console.log('🔴 WebSocket connection closed');
});

