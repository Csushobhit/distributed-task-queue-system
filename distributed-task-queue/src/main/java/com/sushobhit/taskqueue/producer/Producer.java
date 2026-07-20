package com.sushobhit.taskqueue.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.sushobhit.taskqueue.common.ConnectionManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sushobhit.taskqueue.message.TaskMessage;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sushobhit.taskqueue.common.MetricsManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;

@Component
public class Producer implements AutoCloseable {
	
	private static final Logger LOGGER =
	        LoggerFactory.getLogger(Producer.class);

    private static final String EXCHANGE_NAME = "tasks_exchange";
    private static final int MAX_QUEUE_PRIORITY = 10;

    private final Channel channel;

    private final ConcurrentNavigableMap<Long, TaskMessage>
            outstandingConfirms =
            new ConcurrentSkipListMap<>();

    private static final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(
                    new JavaTimeModule());
    
    private final MeterRegistry meterRegistry =
            MetricsManager.getRegistry();

    private final Counter tasksProducedCounter =
            Counter.builder(
                    "tasks.produced.total")
                    .description(
                            "Total number of tasks produced")
                    .register(
                            meterRegistry);

    private final Counter tasksProduceFailedCounter =
            Counter.builder(
                    "tasks.produce.failed.total")
                    .description(
                            "Total number of failed task publications")
                    .register(
                            meterRegistry);
    
    public Producer() throws IOException, TimeoutException {

        ConnectionManager.initialize();

        Connection connection =
                ConnectionManager.getConnection();

        LOGGER.info(
                "Producer obtained connection: "
                        + connection);

        try {

            this.channel =
                    connection.createChannel();

            channel.confirmSelect();

            channel.addConfirmListener(
                    new ConfirmListener() {

                        @Override
                        public void handleAck(
                                long deliveryTag,
                                boolean multiple) {

                            try {

                                handleConfirmation(
                                        deliveryTag,
                                        multiple,
                                        true);

                            } catch (Throwable t) {

                            	LOGGER.error(
                                        "Error while processing ACK: "
                                                + t.getMessage());
                            }
                        }

                        @Override
                        public void handleNack(
                                long deliveryTag,
                                boolean multiple) {

                            try {

                                handleConfirmation(
                                        deliveryTag,
                                        multiple,
                                        false);

                            } catch (Throwable t) {

                            	LOGGER.error(
                                        "Error while processing NACK: "
                                                + t.getMessage());
                            }
                        }
                    });

            LOGGER.info(
                    "Producer created channel: "
                            + this.channel);

            channel.exchangeDeclare(
                    EXCHANGE_NAME,
                    BuiltinExchangeType.DIRECT,
                    true,
                    false,
                    null);

            LOGGER.info(
                    "Exchange declared successfully: "
                            + EXCHANGE_NAME);

        } catch (IOException e) {

        	LOGGER.error(
                    "Failed to create RabbitMQ channel or declare exchange.");

            throw new IOException(
                    "Failed to create RabbitMQ channel or declare exchange.",
                    e);
        }

        LOGGER.info(
                "Producer ready for publishing.");
    }

    private void handleConfirmation(
            long deliveryTag,
            boolean multiple,
            boolean isAck) {

        String confirmationType =
                isAck ? "ACK" : "NACK";

        try {

            ConcurrentNavigableMap<Long, TaskMessage>
                    confirmedMessages;

            if (multiple) {

                confirmedMessages =
                        outstandingConfirms.headMap(
                                deliveryTag,
                                true);

            } else {

                TaskMessage task =
                        outstandingConfirms.get(
                                deliveryTag);

                if (task != null) {

                    confirmedMessages =
                            new ConcurrentSkipListMap<>(
                                    Collections.singletonMap(
                                            deliveryTag,
                                            task));

                } else {

                    confirmedMessages =
                            new ConcurrentSkipListMap<>();
                }
            }

            if (confirmedMessages.isEmpty()
                    && !multiple) {

            	LOGGER.error(
                        confirmationType
                                + " received for unknown deliveryTag: "
                                + deliveryTag);

                return;
            }

            for (Map.Entry<Long, TaskMessage> entry
                    : confirmedMessages.entrySet()) {

                Long currentTag =
                        entry.getKey();

                TaskMessage task =
                        entry.getValue();

                if (isAck) {

                	LOGGER.info(
                            "Broker ACK received. TaskId="
                                    + task.getTaskId()
                                    + ", DeliveryTag="
                                    + currentTag);

                } else {

                	LOGGER.error(
                            "Broker NACK received. TaskId="
                                    + task.getTaskId()
                                    + ", DeliveryTag="
                                    + currentTag);
                }
            }

            if (!confirmedMessages.isEmpty()) {

                if (multiple) {

                    confirmedMessages.clear();

                } else {

                    outstandingConfirms.remove(
                            deliveryTag);
                }
            }

        } catch (Exception e) {

        	LOGGER.error(
                    "Error processing publisher confirmation: "
                            + e.getMessage());
        }
    }

    public void sendTask(TaskMessage task)
            throws IOException {

        if (this.channel == null
                || !this.channel.isOpen()) {

            throw new IOException(
                    "Channel not available for publishing.");
        }

        if (task == null) {

            throw new IllegalArgumentException(
                    "TaskMessage cannot be null.");
        }

        if (task.getTaskType() == null
                || task.getTaskType().trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "Task type cannot be null or empty.");
        }

        if (task.getTaskId() == null) {

            task.setTaskId(
                    UUID.randomUUID());
        }

        String routingKey =
                task.getTaskType();

        byte[] messageBody;

        try {

            String jsonMessage =
                    objectMapper.writeValueAsString(
                            task);

            messageBody =
                    jsonMessage.getBytes(
                            StandardCharsets.UTF_8);

        } catch (Exception e) {
        	
        	tasksProduceFailedCounter.increment();
            throw new IOException(
                    "Failed to serialize task message.",
                    e);
        }

        Integer requestedPriority =
                task.getPriority();

        int priorityToSend = 0;

        if (requestedPriority != null) {

            if (requestedPriority < 0) {

                priorityToSend = 0;

            } else if (requestedPriority > MAX_QUEUE_PRIORITY) {

                priorityToSend = MAX_QUEUE_PRIORITY;

            } else {

                priorityToSend = requestedPriority;
            }
        }

        AMQP.BasicProperties properties =
                MessageProperties.PERSISTENT_TEXT_PLAIN
                        .builder()
                        .contentType("application/json")
                        .deliveryMode(2)
                        .priority(priorityToSend)
                        .build();

        long deliveryTag = -1;

        try {

            deliveryTag =
                    channel.getNextPublishSeqNo();

            outstandingConfirms.put(
                    deliveryTag,
                    task);

            channel.basicPublish(
                    EXCHANGE_NAME,
                    routingKey,
                    properties,
                    messageBody
            );
            tasksProducedCounter.increment();

            LOGGER.info(
                    "Task published successfully. TaskId="
                            + task.getTaskId()
                            + ", DeliveryTag="
                            + deliveryTag
                            + ", RoutingKey="
                            + routingKey);

        } catch (IOException e) {
        	
        	tasksProduceFailedCounter.increment();
            if (deliveryTag != -1) {

                outstandingConfirms.remove(
                        deliveryTag);
            }

            throw new IOException(
                    "Error during message publishing.",
                    e);
        } catch (Exception e) {
        	
        	tasksProduceFailedCounter.increment();
            if (deliveryTag != -1) {

                outstandingConfirms.remove(
                        deliveryTag);
            }

            throw new RuntimeException(
                    "Unexpected error while publishing task.",
                    e);
        }
    }

    @Override
    public void close()
            throws IOException,
            TimeoutException {

        if (!outstandingConfirms.isEmpty()) {

        	LOGGER.error(
                    "Closing producer with "
                            + outstandingConfirms.size()
                            + " unconfirmed messages.");

            outstandingConfirms.clear();
        }

        if (this.channel != null
                && this.channel.isOpen()) {

        	LOGGER.info(
                    "Closing producer channel...");

            this.channel.close();
        }

        LOGGER.info(
                "Producer closed.");
    }
}