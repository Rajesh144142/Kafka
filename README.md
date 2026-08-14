# Kafka Microservices & Error-Handling Demo

This workspace is a hands-on learning project showcasing an **Event-Driven Microservices Architecture** built with Node.js, PostgreSQL, Docker, and Apache Kafka. 

It demonstrates core production-grade event streaming concepts: decoupling, partitioning keys, message archiving, and fault tolerance via a **Dead Letter Queue (DLQ)**.

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

### 1. Central Broker Hub (Docker)
* **Kafka Broker**: The event storage backbone.
* **Kafka-UI**: Web-based administration dashboard available at `http://localhost:8080` to monitor topics, offsets, and consumer groups.

### 2. User Service (`services/user-service/`)
* **Role**: Rest API and **Kafka Producer**.
* **Port**: `3000`.
* **Action**: Saves user info to PostgreSQL and publishes a `USER_CREATED` event to the `user-signups` topic.
* **Partition Key**: Emits messages using the database `user.id` as the message key. This guarantees that all events for a given user go to the same partition, enforcing strict ordering.

### 3. Payment Service (`services/payment-service/`)
* **Role**: **Kafka Consumer** and **Producer (DLQ + Payment Events)**.
* **Port**: `3001`.
* **Action**: Subscribes to the `user-signups` topic under `payment-service-group`. Simulates initializing user wallets.
* **Fault Isolation (DLQ)**: If a message contains an invalid email address (specifically containing the word `"fail"`), it throws an error, packages the message alongside the stack trace diagnostic metadata, and sends it to the `user-signups-dlq` topic.
* **Payment Processing**: Exposes `POST /api/payments` endpoint which publishes a `PAYMENT_PROCESSED` event to the `payment-events` topic.

### 4. Notification Service (`services/notification-service/`)
* **Role**: **Kafka Consumer**.
* **Port**: `3002`.
* **Action**: Subscribes to `user-signups` under `notification-service-group` (welcome emails) and `payment-events` under `notification-payment-group` (payment receipt emails).

### 5. Archive Service (`services/archive-service/`)
* **Role**: **Kafka Consumer**.
* **Port**: `3003`.
* **Action**: Subscribes to the `user-signups` topic under `s3-archiver-group`. Writes events to local mock S3 storage (`archive/s3-bucket/`).
* **Environment Toggle Flag**: Can be enabled/disabled using `ENABLE_S3_ARCHIVE=true/false` in `.env`.

---

## ⚙️ Environment Configuration

Copy the template to set up your local variables:
```bash
cp .env.example .env
```
*Note: Make sure to copy this `.env` file into both `services/user-service/`, `services/payment-service/`, `services/notification-service/`, and `services/archive-service/` as needed.*

---

## 🚀 Running the Project

1. **Start the Infrastructure (Docker)**:
   ```bash
   docker compose up -d
   ```
2. **Start all Microservices Concurrently**:
   Run the following command at the root of the project:
   ```bash
   npm run dev
   ```
   *This uses `concurrently` to launch nodemon in all four service folders simultaneously.*

---

## 🧪 Testing the Pipelines

### Case 1: Standard Success Pipeline
Send a valid user signup payload:
```bash
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Valid User", "email": "valid@test.com"}'
```
**Expected Outcome**:
* `user-service` writes the user to the DB and publishes the event.
* `payment-service` initializes the wallet balance.
* `notification-service` logs welcome email delivery.
* `archive-service` writes a new JSON file to the local mock S3 folder (`archive/s3-bucket/`).

### Case 2: Dead Letter Queue (DLQ) Trigger
Send a payload with `"fail"` in the email to trigger a simulated processing crash:
```bash
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Bad User", "email": "fail@dlq.com"}'
```
**Expected Outcome**:
* `payment-service` throws a validation error, catches it, and publishes the diagnostics to the `user-signups-dlq` topic. It then resumes processing without blocking.
* You can open **`http://localhost:8080`** and inspect the `user-signups-dlq` messages to see the original payload and the error stack trace.
* `notification-service` and `archive-service` are unaffected and successfully process their copies of the message.
