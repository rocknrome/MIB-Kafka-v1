const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);

const sendEvent = (topic, eventType, data) => {
  const payloads = [
    { topic, messages: JSON.stringify({ eventType, data }), partition: 0 },
  ];

  producer.send(payloads, (err, data) => {
    if (err) {
      console.error('Error sending event', err);
    } else {
      console.log('Event sent', data);
    }
  });
};

producer.on('ready', () => {
  console.log('Kafka Producer is connected and ready.');

  // Create a client event
  sendEvent('client-events', 'CLIENT_CREATED', { clientId: 1, name: 'Client A' });
});

producer.on('error', (error) => {
  console.error('Error in Kafka Producer', error);
});
