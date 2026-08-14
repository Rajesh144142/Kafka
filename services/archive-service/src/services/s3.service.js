const fs = require('fs');
const path = require('path');

// Simulate S3 client
// In production, you would install the SDK: npm install @aws-sdk/client-s3
// const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
// const s3 = new S3Client({ region: 'us-east-1' });

const bucketName = process.env.S3_BUCKET_NAME || 'my-kafka-archive-bucket';

/**
 * Simulates uploading a Kafka message to an S3 Bucket.
 * @param {string} topic - The Kafka topic the message came from
 * @param {string} key - The Kafka message key (optional)
 * @param {string} message - The message payload (string format)
 */
const uploadToS3 = async (topic, key, message) => {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const dateStr = new Date().toISOString().split('T')[0]; // yyyy-mm-dd
  const filename = `${timestamp}_${key || 'nokey'}.json`;

  // Local directory structure simulating S3 buckets (e.g. archive/s3-bucket/topic/date/file)
  const localS3Path = path.join(__dirname, '..', '..', 'archive', 's3-bucket', topic, dateStr);

  try {
    // 1. Local Simulation (Writes to filesystem)
    if (!fs.existsSync(localS3Path)) {
      fs.mkdirSync(localS3Path, { recursive: true });
    }
    const fullPath = path.join(localS3Path, filename);
    
    // Parse message if it is stringified JSON, then write it pretty-printed
    let parsedMessage;
    try {
      parsedMessage = JSON.parse(message);
    } catch {
      parsedMessage = { raw: message };
    }
    
    fs.writeFileSync(fullPath, JSON.stringify(parsedMessage, null, 2));

    console.log(`🗄️ [S3 Archive Mock] Uploaded to s3://${bucketName}/${topic}/${dateStr}/${filename}`);

    /*
    // 2. Production Code (AWS SDK v3)
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: `${topic}/${dateStr}/${filename}`,
      Body: message,
      ContentType: 'application/json',
    });
    await s3.send(command);
    console.log(`🗄️ [S3 Archive AWS] Uploaded successfully to AWS S3`);
    */

  } catch (error) {
    console.error('❌ Error archiving message to S3:', error);
  }
};

module.exports = { uploadToS3 };
