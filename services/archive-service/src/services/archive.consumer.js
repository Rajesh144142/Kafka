const { runConsumer } = require('../../../../shared/kafka/consumer');
const { USER_SIGNUPS } = require('../../../../shared/kafka/topic');
const { kafka } = require('../config/kafka');
const s3Service = require('./s3.service');

const initArchiveConsumer = async () => {
  const isEnabled = process.env.ENABLE_S3_ARCHIVE === 'true';

  if (!isEnabled) {
    console.log('🗄️ S3 Archival is DISABLED (flag is false in .env). Skipping consumer startup.');
    return;
  }

  console.log('🗄️ S3 Archival is ENABLED. Starting consumer...');

  await runConsumer(kafka, {
    topic: USER_SIGNUPS,
    groupId: 's3-archiver-group',
    onMessage: async (messageStr, key) => {
      await s3Service.uploadToS3(USER_SIGNUPS, key, messageStr);
    },
  });
};

module.exports = { initArchiveConsumer };
