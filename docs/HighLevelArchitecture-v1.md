```mermaid
flowchart LR

    PM[ProducerMain]
    P[Producer]

    CM[ConsumerMain]
    C[Consumer]

    RMQ[(RabbitMQ)]

    PM --> P
    P --> RMQ

    RMQ --> C
    CM --> C
```