const { runConsumer } = require('../kafka/consumer');
const { USER_SIGNUPS } = require('../kafka/topic');

const initEmailConsumer = async () => {
  await runConsumer({
    topic: USER_SIGNUPS,
    groupId: 'email-service-group',
    onMessage: async (messageStr) => {
      console.log(`📧 Email consumer received message: ${messageStr}`);
      // Add email sending logic here
    },
  });
};

module.exports = { initEmailConsumer };
