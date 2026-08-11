const { runConsumer } = require('../kafka/consumer');
const { USER_SIGNUPS, USER_SIGNUPS_DLQ } = require('../kafka/topic');
const { producer } = require('../config/kafka');

const initPaymentConsumer = async () => {
  console.log('💳 Payment consumer is starting...');
  await runConsumer({
    topic: USER_SIGNUPS,
    groupId: 'payment-service-group',
    onMessage: async (messageStr, key) => {
      try {
        const eventData = JSON.parse(messageStr);
        if (eventData.event === 'USER_CREATED') {
          const user = eventData.payload;
          
          // Validation: Simulate an error if the email contains "fail" or is missing
          if (!user.email || user.email.includes('fail')) {
            throw new Error(`Invalid email address format: "${user.email}"`);
          }

          console.log(`💳 [Payment Service] Initializing wallet for user ${user.name} (ID: ${user.id}, Email: ${user.email})...`);
          console.log(`✅ [Payment Service] Wallet initialized with $0.00 balance for user ID: ${user.id}`);
        }
      } catch (err) {
        console.error('❌ [Payment Service] Error processing message, redirecting to DLQ:', err.message);
        
        try {
          const dlqPayload = {
            originalMessage: messageStr,
            error: err.message,
            stack: err.stack,
            timestamp: new Date().toISOString(),
            service: 'payment-service',
          };
          
          // Send to Dead Letter Queue
          await producer.send({
            topic: USER_SIGNUPS_DLQ,
            messages: [{ 
              key: key ? String(key) : null,
              value: JSON.stringify(dlqPayload) 
            }],
          });
          
          console.warn(`🚨 [Payment Service] Successfully redirected message to DLQ topic: ${USER_SIGNUPS_DLQ}`);
        } catch (dlqErr) {
          console.error('❌ [Payment Service] Failed to send message to DLQ:', dlqErr);
        }
      }
    },
  });
};

module.exports = { initPaymentConsumer };
