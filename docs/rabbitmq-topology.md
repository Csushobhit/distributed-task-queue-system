# RabbitMQ Topology Design

## Project

Distributed Task Queue System

## Goal

Build a distributed task processing system where:

- Producers submit tasks.
- RabbitMQ stores and routes tasks.
- Workers process tasks asynchronously.
- Failed tasks are moved to a Dead Letter Queue (DLQ).

---

# Core RabbitMQ Components

## Virtual Host

```text
task_queue_vhost
```

### Why?

Provides isolation.

Without virtual hosts:

```text
RabbitMQ
├── Project A Queues
├── Project B Queues
├── Project C Queues
```

With virtual hosts:

```text
RabbitMQ
├── /
├── task_queue_vhost
└── another_project_vhost
```

Each project gets its own namespace.

---

## RabbitMQ User

```text
task_user
```

### Why?

Applications should not use the default:

```text
guest / guest
```

Dedicated users provide better security and separation.

---

# Message Structure

## TaskMessage

```java
UUID taskId;
String taskType;
Map<String, Object> payload;
Instant timestamp;
int retryCount;
```

### Purpose

| Field | Purpose |
|---------|---------|
| taskId | Unique task identifier |
| taskType | Determines task processing type |
| payload | Actual task data |
| timestamp | Creation time |
| retryCount | Tracks retry attempts |

---

# Exchange Design

## Main Exchange

### Name

```text
tasks_exchange
```

### Type

```text
Direct
```

### Why Direct Exchange?

Direct exchanges route messages using exact matching.

Example:

```text
Routing Key = send_email
Binding Key = send_email
```

Message is routed successfully.

### Alternatives Considered

#### Fanout Exchange

Routes messages to all queues.

Not suitable because:

```text
send_email
```

would go to:

```text
email_queue
image_queue
report_queue
```

#### Topic Exchange

Supports wildcard routing.

Example:

```text
tasks.email.*
tasks.image.#
```

Useful for complex routing.

Not needed for current requirements.

---

# Queue Design

## Email Queue

```text
task_queue_email
```

Handles:

```text
send_email
```

tasks.

---

## Image Processing Queue

```text
task_queue_image_processing
```

Handles:

```text
resize_image
```

tasks.

---

# Routing Strategy

## Rule

```text
routingKey = taskType
bindingKey = taskType
```

### Example

Task:

```text
taskType = send_email
```

Producer publishes:

```text
routingKey = send_email
```

Queue binding:

```text
bindingKey = send_email
```

RabbitMQ routes the message to:

```text
task_queue_email
```

---

# Dead Lettering Design

## Dead Letter Exchange

### Name

```text
tasks_dlx
```

### Type

```text
Fanout
```

### Why Fanout?

Failed messages do not require task-specific routing.

We simply want:

```text
All Failed Messages
        ↓
Dead Letter Queue
```

---

## Dead Letter Queue

### Name

```text
tasks_dlq
```

### Purpose

Stores messages that could not be processed successfully.

Examples:

- Repeated processing failures
- Message rejection
- Business logic errors

---

# Complete RabbitMQ Topology

```text
task_queue_vhost

├── tasks_exchange (Direct)
│
│   ├── send_email
│   │        │
│   │        ▼
│   │   task_queue_email
│   │
│   └── resize_image
│            │
│            ▼
│     task_queue_image_processing
│
├── tasks_dlx (Fanout)
│
└── tasks_dlq
```

---

# Normal Message Flow

```text
Producer
   │
   ▼
tasks_exchange
   │
Routing Key = send_email
   │
   ▼
task_queue_email
   │
   ▼
Email Worker
```

---

# Failed Message Flow

```text
task_queue_email
      │
      ▼
Message Rejected
(requeue = false)
      │
      ▼
tasks_dlx
      │
      ▼
tasks_dlq
```

---

# Real-World Analogy

## Producer

Person sending a letter.

```text
Customer
   ↓
Post Office
```

---

## Exchange

Sorting center.

Determines where messages should go.

```text
Letter
   ↓
Sorting Center
   ↓
Correct Mailbox
```

---

## Queue

Mailbox.

Stores messages until someone picks them up.

---

## Consumer

Postman.

Processes messages from a mailbox.

---

## DLQ

Undelivered mail section.

Messages that could not be delivered successfully are stored here.

---

# Design Goals Achieved

- Asynchronous Processing
- Producer / Consumer Architecture
- Message Routing
- Queue Isolation
- Failure Handling
- Dead Letter Queues
- Scalable Worker Architecture
- Distributed Systems Fundamentals