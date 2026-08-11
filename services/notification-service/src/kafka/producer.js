const { producer } = require('../config/kafka');

const produceMessage = async (topic, message, key = null) => {
    const payload = {
        value: JSON.stringify(message),
    };
    
    if (key !== null && key !== undefined) {
        payload.key = String(key);
    }

    await producer.send({
        topic,
        messages: [payload],
    });
};

module.exports = { produceMessage };
