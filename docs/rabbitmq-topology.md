# RabbitMQ Topology

## Components

### VHost

```text
task_queue_vhost
```

### User

```text
task_user
```

### Main Exchange

```text
Name : tasks_exchange
Type : Direct
```

### Dead Letter Exchange

```text
Name : tasks_dlx
Type : Fanout
```

### Queues

```text
task_queue_email
task_queue_image_processing
tasks_dlq
```

---

# Task Message

```java
UUID taskId;
String taskType;
Map<String, Object> payload;
Instant timestamp;
int retryCount;
```

---

# Routing Strategy

```text
routingKey = taskType
bindingKey = taskType
```

Examples:

```text
send_email   -> task_queue_email

resize_image -> task_queue_image_processing
```

---

# Topology Diagram

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

# Normal Flow

```text
Producer
   │
   ▼
tasks_exchange
   │
   ▼
task_queue_*
   │
   ▼
Worker
```

---

# Failure Flow

```text
Worker
   │
   ▼
Message Rejected
   │
   ▼
tasks_dlx
   │
   ▼
tasks_dlq
```

---

# Design Decisions

| Component | Choice | Reason |
|------------|---------|---------|
| Main Exchange | Direct | Route by task type |
| DLX | Fanout | Send all failed messages to one place |
| Multiple Queues | Yes | Independent scaling per task type |
| DLQ | tasks_dlq | Store failed tasks |
| VHost | task_queue_vhost | Resource isolation |
