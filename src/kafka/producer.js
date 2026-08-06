const { producer } = require('../config/kafka');

const produceMessage = async (topic, message) => {
    await producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }],
    });
};

module.exports = { produceMessage };
