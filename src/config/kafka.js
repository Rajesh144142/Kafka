const { Kafka, Partitioners } = require('kafkajs');

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

const kafka = new Kafka({
  clientId: 'kafka-learning',
  brokers: process.env.KAFKA_BROKERS.split(','),
  createPartitioner: Partitioners.LegacyPartitioner,
  retry: {
    retries: 10,
    initialRetryTime: 300,
    factor: 0.2,
  },
});

const producer = kafka.producer();

const initKafka = async () => {
  const admin = kafka.admin();
  await admin.connect();

  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: 'user-signups',
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
    });
  } catch (error) {
    const message = error.message || '';
    if (
      error.type === 'TOPIC_ALREADY_EXISTS' ||
      /already exists/i.test(message) ||
      /Topic creation errors/i.test(message)
    ) {
      // Topic already exists or creation returned a non-fatal error; continue.
    } else {
      throw error;
    }
  } finally {
    await admin.disconnect();
  }

  // Connect the global long-lived producer
  await producer.connect();
  console.log('⚡ Kafka Producer connected successfully');
};

const shutdownKafka = async () => {
  console.log('🔌 Shutting down Kafka producer...');
  await producer.disconnect();
};

module.exports = {
  kafka,
  producer,
  initKafka,
  shutdownKafka,
};
