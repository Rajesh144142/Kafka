require('dotenv').config();

const app = require('./app');
const db = require('./config/database');
const { initKafka, shutdownKafka } = require('./config/kafka');

const PORT = parseInt(process.env.PORT, 10);

const start = async () => {
  // 1. Initialize databases and connections
  await db.initDb();
  await initKafka();

  // 2. Start Web server
  const server = app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
  });

  // 3. Graceful Shutdown handlers
  const handleShutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
    
    // Stop accepting new HTTP requests
    server.close(() => console.log('HTTP server closed.'));
    
    try {
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
  console.error('❌ Failed to start server:', error);
  process.exit(1);
});
