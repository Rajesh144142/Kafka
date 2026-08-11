const { runConsumer } = require('../kafka/consumer');
const { USER_SIGNUPS } = require('../kafka/topic');

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const initNotificationConsumer = async () => {
  console.log('✉️ Notification consumer is starting...');
  await runConsumer({
    topic: USER_SIGNUPS,
    groupId: 'notification-service-group',
    onMessage: async (messageStr) => {
      try {
        const eventData = JSON.parse(messageStr);
        if (eventData.event === 'USER_CREATED') {
          const user = eventData.payload;
          
          console.log(`📬 [Notification Service] Sending welcome email to ${user.name} (${user.email})...`);
          
          // Simulate network delay for SMTP call
          await delay(1500);
          
          console.log(`✉️ [Notification Service] Welcome email successfully sent to ${user.email}!`);
        }
      } catch (err) {
        console.error('❌ [Notification Service] Error processing message:', err);
      }
    },
  });
};

module.exports = { initNotificationConsumer };
