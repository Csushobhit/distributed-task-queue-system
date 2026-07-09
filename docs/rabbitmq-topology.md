# RabbitMQ Topology

## Core Components

| Component | Value |
|------------|---------|
| VHost | `task_queue_vhost` |
| User | `task_user` |
| Main Exchange | `tasks_exchange (Direct)` |
| DLX | `tasks_dlx (Fanout)` |
| DLQ | `tasks_dlq` |

---

## RabbitMQ Topology

```mermaid
flowchart LR

    P[Producer]

    EX["tasks_exchange<br/>Direct Exchange"]

    EMAIL["task_queue_email"]
    IMAGE["task_queue_image_processing"]

    EW[Email Worker]
    IW[Image Worker]

    P --> EX

    EX -->|send_email| EMAIL
    EX -->|resize_image| IMAGE

    EMAIL --> EW
    IMAGE --> IW
```

---

## Dead Letter Flow

```mermaid
flowchart LR

    EMAIL["task_queue_email"]
    IMAGE["task_queue_image_processing"]

    DLX["tasks_dlx<br/>Fanout Exchange"]
    DLQ["tasks_dlq"]

    EMAIL -. reject .-> DLX
    IMAGE -. reject .-> DLX

    DLX --> DLQ
```

---

## Complete Flow

```mermaid
flowchart LR

    P[Producer]

    EX["tasks_exchange<br/>Direct"]

    EMAIL["task_queue_email"]
    IMAGE["task_queue_image_processing"]

    EW[Email Worker]
    IW[Image Worker]

    DLX["tasks_dlx<br/>Fanout"]
    DLQ["tasks_dlq"]

    P --> EX

    EX -->|send_email| EMAIL
    EX -->|resize_image| IMAGE

    EMAIL --> EW
    IMAGE --> IW

    EMAIL -. failed .-> DLX
    IMAGE -. failed .-> DLX

    DLX --> DLQ
```

---

## Design Decisions

| Choice | Reason |
|----------|----------|
| Direct Exchange | Exact routing by task type |
| Multiple Queues | Independent scaling and isolation |
| Fanout DLX | Route all failed messages to one place |
| DLQ | Preserve failed tasks for investigation |
| Dedicated VHost | Resource isolation |
