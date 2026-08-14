# Walkthrough - Kafka Integration Centralization & Microservices Demo

We have completed the refactoring and centralized the duplicate Kafka configurations into a shared package folder, keeping our code clean and easily maintainable.

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    Client[Client / Postman / cURL] -->|POST /api/users<br>Port 3000| US["User Service (Port 3000)"]
    Client -->|POST /api/payments<br>Port 3001| PS["Payment Service (Port 3001)"]
    Client -->|GET /health<br>Port 3002| NS["Notification Service (Port 3002)"]
    Client -->|GET /health<br>Port 3003| AS["Archive Service (Port 3003)"]

    subgraph DBCluster ["Database Hub"]
        DB[(PostgreSQL)]
    end

    US -->|Write User| DB
    PS -.->|Shared Schema DB init| DB
    NS -.->|Shared Schema DB init| DB
    AS -.->|Shared Schema DB init| DB

    subgraph SharedCode ["Shared Configuration"]
        SC["shared/kafka/"]
    end
    US -.->|Require client factory| SC
    PS -.->|Require client factory| SC
    NS -.->|Require client factory| SC
    AS -.->|Require client factory| SC

    subgraph KafkaBroker ["Kafka Event Hub"]
        Topic1["user-signups"]
        Topic2["user-signups-dlq"]
        Topic3["payment-events"]
    end

    US -->|Publish: USER_CREATED| Topic1
    Topic1 -->|Group: payment-service-group| PS
    Topic1 -->|Group: notification-service-group| NS
    Topic1 -->|Group: s3-archiver-group| AS

    PS -->|On Validation Failure| Topic2
    PS -->|Publish: PAYMENT_PROCESSED| Topic3
    Topic3 -->|Group: notification-payment-group| NS

    AS -->|Write JSON Archive| S3[(Mock S3 Bucket)]
    
    subgraph Management ["Management"]
        KUI[Kafka-UI (Port 8080)] <-->|Monitor| KafkaBroker
    end
```

### Centralized Config Files
Instead of having duplicate `kafka.js`, `topic.js`, and `consumer.js` files inside each microservice, we consolidated them into a single root folder:
* **[shared/kafka/kafka.js](file:///d:/kafka-demo/shared/kafka/kafka.js)**: Holds the dynamic client creation factory (`createKafkaClient(clientId)`), log formatter, and environment configuration.
* **[shared/kafka/topic.js](file:///d:/kafka-demo/shared/kafka/topic.js)**: Contains all topic name constants in one source of truth.
* **[shared/kafka/consumer.js](file:///d:/kafka-demo/shared/kafka/consumer.js)**: Exports `runConsumer(kafka, ...)` and `shutdownConsumers()` functions.

---

## ⚙️ Components Configured

### 1. User Service (`services/user-service` on Port `3000`)
* Exposes `POST /api/users` and `GET /api/users` REST endpoints.
* Exposes bulk RBAC APIs for creating roles, permissions, and mappings.
* Publishes user signup events using database `user.id` as the **partitioning key**.

### 2. Payment Service (`services/payment-service` on Port `3001`)
* Subscribes to `user-signups` to initialize wallets.
* Exposes `POST /api/payments` to process payments and publishes a `PAYMENT_PROCESSED` event to the `payment-events` topic.
* **Fault Isolation (DLQ)**: Intercepts validation errors and redirects them to the `user-signups-dlq` topic.

### 3. Notification Service (`services/notification-service` on Port `3002`)
* Subscribes to `user-signups` to send welcome emails.
* Subscribes to `payment-events` to log payment receipts.

### 4. Archive Service (`services/archive-service` on Port `3003`)
* Subscribes to `user-signups` and archives event payloads to `archive/s3-bucket/`.

---

## 📈 Human-Readable Logging
We added a custom log formatter inside the shared Kafka client builder. In your terminal, the logs look like:
* 🟢 `🟢 [KafkaJS] Starting`
* 📥 `Consumer group "payment-service-group" listening on topic "user-signups"`
* 💳 `💳 [Payment Service] Processing payment of USD 99.99 for User ID 14...`
* 📬 `📬 [Notification Service] Sending welcome email to Shared Test User...`
* ✉️ `✉️ [Notification Service] Payment receipt email sent successfully to User ID 14!`
