const { createKafkaClient } = require('../../../../shared/kafka/kafka');
const kafka = createKafkaClient('archive-service');

module.exports = { kafka };
