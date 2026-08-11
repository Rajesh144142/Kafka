const { kafka } = require('../config/kafka');

const activeConsumers = [];

const runConsumer = async ({ topic, groupId, onMessage }) => {
  const consumer = kafka.consumer({ groupId });
  
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value ? message.value.toString() : null;
        const key = message.key ? message.key.toString() : null;
        await onMessage(value, key);
      } catch (err) {
        console.error(`❌ Error processing message on topic ${topic}:`, err);
      }
    },
  });

  activeConsumers.push(consumer);
  console.log(`📥 Consumer group "${groupId}" listening on topic "${topic}"`);
  return consumer;
};

const shutdownConsumers = async () => {
  console.log('🔌 Shutting down active Kafka consumers...');
  for (const consumer of activeConsumers) {
    try {
      await consumer.disconnect();
    } catch (err) {
      console.error('Error disconnecting consumer:', err);
    }
  }
};

module.exports = { runConsumer, shutdownConsumers };
