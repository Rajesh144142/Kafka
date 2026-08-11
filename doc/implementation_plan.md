# Implement Notification Service & Dead Letter Queue (DLQ)

We will implement two new capabilities in our microservices ecosystem:
1. **Notification Service** (`services/notification-service`): A new independent consumer service simulating email notification workflows.
2. **Dead Letter Queue (DLQ)**: Error handling logic inside `payment-service` that redirects invalid or corrupted messages to a dedicated `user-signups-dlq` topic instead of blocking the processing pipeline.

---

## User Review Required

> [!NOTE]
> * We will add a new Kafka topic: `user-signups-dlq`.
> * We will add a new microservice folder: `services/notification-service/`.
> * To test the DLQ, we will trigger a simulated error in `payment-service` if a user signs up with the email `fail@dlq.com` or similar.
> * We will update the root `package.json` to launch the third service concurrently using `npm run dev`.

---

## Open Questions

None.

---

## Proposed Changes

### Topics Configuration

#### [MODIFY] [topic.js](file:///d:/kafka-demo/services/user-service/src/kafka/topic.js) (and payment-service equivalent)
* Export new constant `USER_SIGNUPS_DLQ = 'user-signups-dlq'`.

#### [MODIFY] [kafka.js](file:///d:/kafka-demo/services/user-service/src/config/kafka.js)
* Update `initKafka` to automatically create the `user-signups-dlq` topic on startup.

---

### Notification Service (New Microservice)

#### [NEW] [notification-service files](file:///d:/kafka-demo/services/notification-service/)
* Create `package.json`, `.env`, and database/kafka connections for the new service.
* Create [notification.consumer.js](file:///d:/kafka-demo/services/notification-service/src/services/notification.consumer.js) that listens to `user-signups` under the `notification-service-group` ID.
* Print simulated email triggers with logs.

---

### Payment Service DLQ Integration

#### [MODIFY] [kafka.js](file:///d:/kafka-demo/services/payment-service/src/config/kafka.js)
* Initialize and connect a global Kafka Producer so the Payment Service can publish error events to the DLQ topic.

#### [MODIFY] [payment.consumer.js](file:///d:/kafka-demo/services/payment-service/src/services/payment.consumer.js)
* Introduce validation checks. If the event payload fails verification (e.g. invalid email structure or specific trigger), throw an error.
* Catch errors, format the bad payload with failure metadata, and publish it to the `user-signups-dlq` topic.

---

### Root Configuration

#### [MODIFY] [package.json](file:///d:/kafka-demo/package.json)
* Add `"dev:notification": "npm run dev --prefix services/notification-service"` script.
* Update `"dev"` command to execute all three microservices concurrently.

---

## Verification Plan

### Automated Tests
* None.

### Manual Verification
1. Install dependencies and start the cluster: `npm run dev`.
2. Verify all three services and the DLQ topic are active.
3. **Verify Notification Service**:
   * Send a standard signup request.
   * Check notification logs to see the simulated email delivery success.
4. **Verify DLQ Routing**:
   * Send a signup request with invalid email format (e.g. `fail@dlq.com`).
   * Verify that:
     * `payment-service` catches the error and logs a DLQ redirect warning.
     * The `user-signups-dlq` topic in the Kafka-UI dashboard gets a new message containing the failed payload and the stack trace error.
     * Other consumers (Email/Analytics) process the signup normally.
