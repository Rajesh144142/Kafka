require('dotenv').config();

const db = require('./config/database');
const { initKafka, shutdownKafka } = require('./config/kafka');
const { shutdownConsumers } = require('./kafka/consumer');
const { initPaymentConsumer } = require('./services/payment.consumer');

const start = async () => {
  console.log('👷 Starting Payment background workers...');
  
  // 1. Initialize database and schemas
  await db.initDb();
  await initKafka();

  // 2. Start consumer groups
  await initPaymentConsumer();

  console.log('🚀 Payment workers are online and running.');

  // 3. Graceful Shutdown handlers
  const handleShutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
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
