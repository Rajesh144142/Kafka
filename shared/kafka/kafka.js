const { Kafka, Partitioners, logLevel } = require('kafkajs');

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

const customLogCreator = (logLevelValue) => {
  return ({ namespace, level, label, log }) => {
    const { message } = log;
    let icon = '⚪';
    if (level === logLevel.ERROR) icon = '🔴';
    else if (level === logLevel.WARN) icon = '🟡';
    else if (level === logLevel.INFO) icon = '🟢';
    else if (level === logLevel.DEBUG) icon = '🔍';

    console.log(`${icon} [KafkaJS] ${message}`);
  };
};

const createKafkaClient = (clientId) => {
  return new Kafka({
    clientId: clientId || 'kafka-learning',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    createPartitioner: Partitioners.LegacyPartitioner,
    logCreator: customLogCreator,
    retry: {
      retries: 10,
      initialRetryTime: 300,
      factor: 0.2,
    },
  });
};

module.exports = {
  createKafkaClient,
  logLevel,
};
