# Refactor Kafka Integration to Production Standards

This plan outlines the refactoring of the Kafka integration in [kafka-demo](file:///d:/kafka-demo) to optimize performance, manage resource lifecycles correctly, and handle graceful shutdown of connections.

## User Review Required

> [!IMPORTANT]
> The producer connection lifecycle is being updated so it connects once at server startup and remains open. This increases performance by several orders of magnitude but requires correct startup wiring in `server.js`.
> Both consumer services (`analytics.consumer.js` and `email.consumer.js`) will now automatically start when the Express server starts.

## Open Questions

* No open questions. We will use the existing configuration broker values (defaults to `localhost:9092`) but add support for `KAFKA_BROKERS` environment variable for flexibility.

## Proposed Changes

---

### Kafka Infrastructure & Lifecycle

#### [MODIFY] [kafka.js](file:///d:/kafka-demo/src/config/kafka.js)
* Initialize a single long-lived `producer` instance and export it.
* Create an `initKafka` function to run the admin client (setup topics) and connect the shared producer.
* Create a `shutdownKafka` function to gracefully disconnect the producer.

#### [MODIFY] [producer.js](file:///d:/kafka-demo/src/kafka/producer.js)
* Remove the per-request instantiate-connect-disconnect flow.
* Import the shared `producer` from `src/config/kafka.js` and call `.send` directly on it.

#### [MODIFY] [consumer.js](file:///d:/kafka-demo/src/kafka/consumer.js)
* Standardize the `runConsumer` helper to instantiate, connect, and subscribe.
* Keep track of instantiated consumers in an `activeConsumers` array.
* Add and export a `shutdownConsumers` function that disconnects all active consumers.
* Add generic error handling/logging for incoming messages.

---

### Consumer Services

#### [MODIFY] [analytics.consumer.js](file:///d:/kafka-demo/src/services/analytics.consumer.js)
* Refactor to use the standard helper `runConsumer` from `src/kafka/consumer.js`.
* Pass the `USER_SIGNUPS` topic constant.

#### [MODIFY] [email.consumer.js](file:///d:/kafka-demo/src/services/email.consumer.js)
* Refactor to use the standard helper `runConsumer` from `src/kafka/consumer.js`.
* Pass the `USER_SIGNUPS` topic constant.

---

### Server Wiring & Process Lifecycle

#### [MODIFY] [server.js](file:///d:/kafka-demo/src/server.js)
* Import `initKafka` and `shutdownKafka` from `src/config/kafka.js`.
* Import consumer initialization functions (`initAnalyticsConsumer`, `initEmailConsumer`).
* Start the consumers after calling `initKafka()`.
* Add `SIGINT` and `SIGTERM` process event listeners to run graceful shutdown sequence (disconnecting consumers and producers, closing the HTTP server).

---

## Verification Plan

### Automated Tests
* We can run the server locally and check startup logs. Since there are no unit tests, we'll focus on manual verification of message flow.

### Manual Verification
1. Start the Kafka container via `docker-compose up -d`.
2. Start the Node.js server using `npm run start` or `npm run dev`.
3. Verify that:
   - Kafka producer connects.
   - Consumers connect and subscribe to `user-signups`.
4. Trigger a user signup via POST to `/api/users`.
5. Check logs to verify:
   - The user is created in the database.
   - The event is published by the producer.
   - Both consumers (`analytics-service-group` and `email-service-group`) receive and print the message.
6. Stop the server (`Ctrl+C` or sending SIGTERM) and verify that the graceful shutdown logs print, indicating consumers and producer disconnected cleanly.
