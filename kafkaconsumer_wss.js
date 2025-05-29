const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

// web socket server instance at post 8080
const wss = new WebSocket.Server({ port: 8080 });

//kafka broker instance at local
const kafka = new Kafka({ clientId: 'kraken-consumer', brokers: ['localhost:9092'] });

// #kafka consumer
const consumer = kafka.consumer({ groupId: 'crypto-group' });

// set of all the clients connected to wss
const clients = new Set();


wss.on('connection', (ws) => {
    console.log('Client connected');

    // add newly connected client to set
    clients.add(ws);

    // removing clients on error or close from set
    ws.on('close', () => {
        console.log('Client disconnected');
        clients.delete(ws);
    });
    ws.on('error', (err) => {
        console.error('WebSocket error:', err);
        clients.delete(ws);
    });
});

(async () => {

    // connecting kafka consumet
    await consumer.connect();
    console.log("🟢 Kafka Consumer Connected");

    // subscribing to kafka topics
    await consumer.subscribe({ topic: 'quotes.crypto', fromBeginning: false });
    console.log("💡 Kafka Topics Subscribed");

    // reading all the messages in subscribed topic without partion
    await consumer.run({
        eachMessage: async ({ message, topic }) => {

            // recieving payload form kafka
            var payload =message.value.toString()
            console.log('✉️', topic, payload);

            // sending recieved payloads to all active customers
            for (let client of clients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(payload);
                }
            }
        }
    });
})();