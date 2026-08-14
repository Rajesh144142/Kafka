const { createKafkaClient } = require('../../../../shared/kafka/kafka');
const { USER_SIGNUPS, ROLE_ASSIGNMENTS, AUDIT_LOGS, USER_SIGNUPS_DLQ, PAYMENT_EVENTS } = require('../../../../shared/kafka/topic');

const kafka = createKafkaClient('payment-service');
const producer = kafka.producer();

const initKafka = async () => {
  const admin = kafka.admin();
  await admin.connect();

  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: USER_SIGNUPS,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: ROLE_ASSIGNMENTS,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: AUDIT_LOGS,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: USER_SIGNUPS_DLQ,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: PAYMENT_EVENTS,
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
