require('dotenv').config();

const db = require('./config/database');
const { shutdownConsumers } = require('./kafka/consumer');
const { initNotificationConsumer } = require('./services/notification.consumer');

const start = async () => {
  console.log('👷 Starting Notification background workers...');
  
  // 1. Initialize database and schemas
  await db.initDb();

  // 2. Start consumer groups
  await initNotificationConsumer();

  console.log('🚀 Notification workers are online and running.');

  // 3. Graceful Shutdown handlers
  const handleShutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
    try {
      await shutdownConsumers();
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
  console.error('❌ Failed to start Notification workers:', error);
  process.exit(1);
});
