const { runConsumer } = require('../kafka/consumer');
const { USER_SIGNUPS } = require('../kafka/topic');

const initAnalyticsConsumer = async () => {
  await runConsumer({
    topic: USER_SIGNUPS,
    groupId: 'analytics-service-group',
    onMessage: async (messageStr) => {
      console.log(`📊 Analytics consumer received message: ${messageStr}`);
      // Add analytics processing logic here
    },
  });
};

module.exports = { initAnalyticsConsumer };
