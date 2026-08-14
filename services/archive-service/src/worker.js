require('dotenv').config();

const express = require('express');
const db = require('./config/database');
const { shutdownConsumers } = require('../../../shared/kafka/consumer');
const { initArchiveConsumer } = require('./services/archive.consumer');

const PORT = parseInt(process.env.PORT, 10) || 3003;

const start = async () => {
  console.log('👷 Starting Archive background workers...');
  
  // 1. Initialize database and schemas
  await db.initDb();

  // 2. Start consumer groups
  await initArchiveConsumer();

  // 3. Start HTTP Express Server (Health Check)
  const app = express();
  app.get('/health', (req, res) => {
    res.json({ status: 'UP', service: 'archive-service' });
  });
  app.get('/', (req, res) => {
    res.send('Archive Service (Kafka Consumer + S3 Archiver)');
  });
  const server = app.listen(PORT, () => {
    console.log(`🚀 HTTP Server running on http://localhost:${PORT}`);
  });

  console.log('🚀 Archive workers are online and running.');

  // 4. Graceful Shutdown handlers
  const handleShutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
    server.close(() => console.log('HTTP server closed.'));
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
  console.error('❌ Failed to start Archive workers:', error);
  process.exit(1);
});
