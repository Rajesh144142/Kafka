require('dotenv').config();

const express = require('express');
const db = require('./config/database');
const { initKafka, shutdownKafka, producer } = require('./config/kafka');
const { shutdownConsumers } = require('../../../shared/kafka/consumer');
const { initPaymentConsumer } = require('./services/payment.consumer');
const { PAYMENT_EVENTS } = require('../../../shared/kafka/topic');

const PORT = parseInt(process.env.PORT, 10) || 3001;

const start = async () => {
  console.log('👷 Starting Payment background workers...');
  
  // 1. Initialize database and schemas
  await db.initDb();
  await initKafka();

  // 2. Start consumer groups
  await initPaymentConsumer();

  // 3. Start HTTP Express Server (Health Check)
  const app = express();
  app.use(express.json());

  app.get('/health', (req, res) => {
    res.json({ status: 'UP', service: 'payment-service' });
  });

  app.get('/', (req, res) => {
    res.send('Payment Service (Kafka Consumer + DLQ)');
  });

  app.post('/api/payments', async (req, res) => {
    try {
      const { userId, amount, currency } = req.body;
      if (!userId || !amount) {
        return res.status(400).json({ error: 'Request body must contain "userId" and "amount".' });
      }

      console.log(`💳 [Payment Service] Processing payment of ${currency || 'USD'} ${amount} for User ID ${userId}...`);

      const eventPayload = {
        event: 'PAYMENT_PROCESSED',
        payload: {
          userId,
          amount,
          currency: currency || 'USD',
          timestamp: new Date().toISOString(),
        }
      };

      // Publish PAYMENT_PROCESSED event
      await producer.send({
        topic: PAYMENT_EVENTS,
        messages: [{
          key: String(userId),
          value: JSON.stringify(eventPayload)
        }]
      });

      console.log(`✅ [Payment Service] Payment processed event published to topic: ${PAYMENT_EVENTS}`);
      res.status(200).json({ status: 'SUCCESS', message: 'Payment event published successfully.' });
    } catch (error) {
      console.error('❌ [Payment Service] Error processing payment endpoint:', error);
      res.status(500).json({ error: error.message });
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`🚀 HTTP Server running on http://localhost:${PORT}`);
  });

  console.log('🚀 Payment workers are online and running.');

  // 4. Graceful Shutdown handlers
  const handleShutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
    server.close(() => console.log('HTTP server closed.'));
    try {
      await shutdownConsumers();
      await shutdownKafka();
      console.log('✅ Graceful shutdown complete.');
      process.exit(0);
    } catch (err) {
      console.error('❌ Error during graceful shutdown:', err);
      process.exit(1);
    }
  };

  process.on('SIGINT', () => handleShutdown('SIGINT'));
  process.on('SIGTERM', () => handleShutdown('SIGTERM'));
};

start().catch((error) => {
  console.error('❌ Failed to start Payment workers:', error);
  process.exit(1);
});
