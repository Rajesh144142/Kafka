const { runConsumer } = require('../../../../shared/kafka/consumer');
const { USER_SIGNUPS, PAYMENT_EVENTS } = require('../../../../shared/kafka/topic');
const { kafka } = require('../config/kafka');

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const initNotificationConsumer = async () => {
  console.log('✉️ Notification signup consumer is starting...');
  await runConsumer(kafka, {
    topic: USER_SIGNUPS,
    groupId: 'notification-service-group',
    onMessage: async (messageStr) => {
      try {
        const eventData = JSON.parse(messageStr);
        if (eventData.event === 'USER_CREATED') {
          const user = eventData.payload;
          console.log(`📬 [Notification Service] Sending welcome email to ${user.name} (${user.email})...`);
          await delay(1500);
          console.log(`✉️ [Notification Service] Welcome email successfully sent to ${user.email}!`);
        }
      } catch (err) {
        console.error('❌ [Notification Service] Error processing signup message:', err);
      }
    },
  });
};

const initPaymentNotificationConsumer = async () => {
  console.log('💳 Notification payment consumer is starting...');
  await runConsumer(kafka, {
    topic: PAYMENT_EVENTS,
    groupId: 'notification-payment-group',
    onMessage: async (messageStr) => {
      try {
        const eventData = JSON.parse(messageStr);
        if (eventData.event === 'PAYMENT_PROCESSED') {
          const payment = eventData.payload;
          console.log(`📬 [Notification Service] Sending payment receipt email to User ID ${payment.userId} for amount ${payment.currency} ${payment.amount}...`);
          await delay(1500);
          console.log(`✉️ [Notification Service] Payment receipt email sent successfully to User ID ${payment.userId}!`);
        }
      } catch (err) {
        console.error('❌ [Notification Service] Error processing payment message:', err);
      }
    },
  });
};

module.exports = { 
  initNotificationConsumer,
  initPaymentNotificationConsumer
};
