# Walkthrough - Notification Service & Dead Letter Queue (DLQ)

We have successfully implemented:
1. **Notification Microservice** (`services/notification-service`): Simulates email delivery for new users.
2. **Dead Letter Queue (DLQ) Pipeline**: Handles failed/malformed events in `payment-service` and routes them to a dedicated `user-signups-dlq` topic.

---

## 1. Notification Microservice
* **Consumer Service**: Located at `services/notification-service`.
* **Consumer Group**: `notification-service-group` listening on `user-signups` topic.
* **Simulated SMTP Logic**: When a user creation event is read, it logs that it is sending an email, pauses for 1.5 seconds, and logs a successful delivery message.
* **Root dev integration**: Updated the root `package.json` to start this service concurrently alongside others.

---

## 2. Dead Letter Queue (DLQ) Pipeline
* **DLQ Topic**: Registered `user-signups-dlq` topic inside [kafka.js](file:///d:/kafka-demo/services/user-service/src/config/kafka.js) topic initialization.
* **Validation & Catch block in `payment-service`**:
  * We modified the payment consumer ([payment.consumer.js](file:///d:/kafka-demo/services/payment-service/src/services/payment.consumer.js)) to check user email formats.
  * If the email is missing or contains the word `"fail"` (e.g., `fail@dlq.com`), it throws a validation error.
  * The `catch` block intercepts the error, wraps the message payload alongside error diagnostics (stack trace, error message, timestamp, originating service), and publishes it to the `user-signups-dlq` topic.
  * The processing pipeline is not blocked; the consumer moves immediately to the next message.

---

## 3. How to Test

### Setup
1. **Stop your active terminal process** (`Ctrl+C`).
2. Run `npm install` at the root to download dependencies, then start the services concurrently:
   ```bash
   npm run dev
   ```

### Test case 1: Standard Signup (Notification Service)
1. In another terminal, trigger a signup:
   ```bash
   curl -X POST http://localhost:3000/api/users -H "Content-Type: application/json" -d '{"name": "Valid User", "email": "valid@test.com"}'
   ```
2. Verify in logs:
   * **`user-service`** registers the user.
   * **`payment-service`** sets up the wallet: `✅ [Payment Service] Wallet initialized...`
   * **`notification-service`** fires the email: `✉️ [Notification Service] Welcome email successfully sent to valid@test.com!`

### Test case 2: Corrupted/Simulated Failure (DLQ Routing)
1. Send a request with a "fail" email address:
   ```bash
   curl -X POST http://localhost:3000/api/users -H "Content-Type: application/json" -d '{"name": "Bad User", "email": "fail@dlq.com"}'
   ```
2. Verify in logs:
   * **`payment-service`** intercepts the invalid email:
     ```text
     ❌ [Payment Service] Error processing message, redirecting to DLQ: Invalid email address format: "fail@dlq.com"
     🚨 [Payment Service] Successfully redirected message to DLQ topic: user-signups-dlq
     ```
   * **`notification-service`** processes the event normally (since it has different business logic and validation requirements):
     ```text
     ✉️ [Notification Service] Welcome email successfully sent to fail@dlq.com!
     ```
3. **Verify in Kafka UI**:
   * Open **[http://localhost:8080](http://localhost:8080)**.
   * Go to **Topics** ➔ select **`user-signups-dlq`** ➔ **Messages**.
   * You will see the diagnostic payload containing the error message and the stack trace!
