const { Client } = require('pg');
const kafka = require('kafka-node');

const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const consumer = new kafka.Consumer(kafkaClient, [{ topic: 'client-events', partition: 0 }], { autoCommit: true });

const pgClient = new Client({
  user: 'user',
  host: 'localhost',
  database: 'testdb',
  password: 'password',
  port: 5432,
});

pgClient.connect();

consumer.on('message', async (message) => {
  const event = JSON.parse(message.value);

  if (event.eventType === 'CLIENT_CREATED') {
    const { clientId, name } = event.data;
    await pgClient.query('INSERT INTO clients (id, name) VALUES ($1, $2)', [clientId, name]);
    console.log('Client created and saved to PostgreSQL');
  }
  // Other event types go here (e.g., CLIENT_UPDATED, CLIENT_DELETED) here
});

consumer.on('error', (error) => {
  console.error('Error in Kafka Consumer', error);
});
