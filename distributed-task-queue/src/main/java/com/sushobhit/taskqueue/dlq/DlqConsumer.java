package com.sushobhit.taskqueue.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.message.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DlqConsumer implements AutoCloseable {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    DlqConsumer.class);

    private static final String DLQ_NAME =
            "tasks_dlq";

    private static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(
                            new JavaTimeModule());

    private final Channel channel;

    private String consumerTag;

    public DlqConsumer()
            throws IOException,
            TimeoutException {

        ConnectionManager.initialize();

        Connection connection =
                ConnectionManager.getConnection();

        this.channel =
                connection.createChannel();

        LOGGER.info(
                "DLQ Consumer created.");
    }

    public void start()
            throws IOException {

        channel.basicQos(
                1);

        consumerTag =
                channel.basicConsume(
                        DLQ_NAME,
                        false,
                        this::handleDelivery,
                        consumerTag -> LOGGER.info(
                                "DLQ Consumer cancelled: {}",
                                consumerTag)
                );

        LOGGER.info(
                "DLQ Consumer started.");
    }

    private void handleDelivery(
            String consumerTag,
            Delivery delivery)
            throws IOException {

        long deliveryTag =
                delivery.getEnvelope()
                        .getDeliveryTag();

        byte[] body =
                delivery.getBody();

        String messageContent =
                new String(
                        body,
                        StandardCharsets.UTF_8);

        LOGGER.info(
                "================ DLQ MESSAGE ================");

        LOGGER.info(
                "DeliveryTag: {}",
                deliveryTag);

        LOGGER.info(
                "Message Content:");

        LOGGER.info(
                "{}",
                messageContent);

        try {

            TaskMessage taskMessage =
                    objectMapper.readValue(
                            messageContent,
                            TaskMessage.class);

            LOGGER.info(
                    "Deserialized Message:");

            LOGGER.info(
                    "{}",
                    taskMessage);

        } catch (Exception e) {

            LOGGER.error(
                    "Failed to deserialize DLQ message.",
                    e);
        }

        logXDeathHeaders(
                deliveryTag,
                delivery.getProperties());

        channel.basicAck(
                deliveryTag,
                false);

        LOGGER.info(
                "DLQ message acknowledged.");

        LOGGER.info(
                "=============================================");
    }

    @SuppressWarnings("unchecked")
    private void logXDeathHeaders(
            long deliveryTag,
            AMQP.BasicProperties properties) {

        Map<String, Object> headers =
                properties.getHeaders();

        if (headers == null
                || !headers.containsKey(
                "x-death")) {

            LOGGER.info(
                    "No x-death header found.");

            return;
        }

        try {

            List<Map<String, Object>> xDeath =
                    (List<Map<String, Object>>)
                            headers.get(
                                    "x-death");

            if (xDeath == null
                    || xDeath.isEmpty()) {

                LOGGER.info(
                        "x-death header empty.");

                return;
            }

            Map<String, Object> deathEntry =
                    xDeath.get(
                            0);

            LOGGER.info(
                    "Dead Letter Details:");

            LOGGER.info(
                    "Reason: {}",
                    deathEntry.get(
                            "reason"));

            LOGGER.info(
                    "Count: {}",
                    deathEntry.get(
                            "count"));

            LOGGER.info(
                    "Queue: {}",
                    deathEntry.get(
                            "queue"));

            LOGGER.info(
                    "Exchange: {}",
                    deathEntry.get(
                            "exchange"));

            LOGGER.info(
                    "Routing Keys: {}",
                    deathEntry.get(
                            "routing-keys"));

        } catch (Exception e) {

            LOGGER.error(
                    "Failed to parse x-death header.",
                    e);
        }
    }

    @Override
    public void close() {

        try {

            if (consumerTag != null
                    && channel.isOpen()) {

                channel.basicCancel(
                        consumerTag);
            }

            if (channel.isOpen()) {

                channel.close();
            }

        } catch (Exception e) {

            LOGGER.error(
                    "Failed to close DLQ consumer.",
                    e);
        }
    }
}