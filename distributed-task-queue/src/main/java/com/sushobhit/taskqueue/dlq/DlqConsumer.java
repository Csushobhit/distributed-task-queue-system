package com.sushobhit.taskqueue.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;

import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.message.TaskMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DlqConsumer implements AutoCloseable {

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

        System.out.println(
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
                        consumerTag -> System.out.println(
                                "DLQ Consumer cancelled: "
                                        + consumerTag)
                );

        System.out.println(
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

        System.out.println(
                "\n================ DLQ MESSAGE ================");

        System.out.println(
                "DeliveryTag: "
                        + deliveryTag);

        System.out.println(
                "Message Content:");

        System.out.println(
                messageContent);

        try {

            TaskMessage taskMessage =
                    objectMapper.readValue(
                            messageContent,
                            TaskMessage.class);

            System.out.println(
                    "Deserialized Message:");

            System.out.println(
                    taskMessage);

        } catch (Exception e) {

            System.err.println(
                    "Failed to deserialize DLQ message.");
        }

        logXDeathHeaders(
                deliveryTag,
                delivery.getProperties());

        channel.basicAck(
                deliveryTag,
                false);

        System.out.println(
                "DLQ message acknowledged.");

        System.out.println(
                "=============================================\n");
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

            System.out.println(
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

                System.out.println(
                        "x-death header empty.");

                return;
            }

            Map<String, Object> deathEntry =
                    xDeath.get(
                            0);

            System.out.println(
                    "Dead Letter Details:");

            System.out.println(
                    "Reason: "
                            + deathEntry.get(
                            "reason"));

            System.out.println(
                    "Count: "
                            + deathEntry.get(
                            "count"));

            System.out.println(
                    "Queue: "
                            + deathEntry.get(
                            "queue"));

            System.out.println(
                    "Exchange: "
                            + deathEntry.get(
                            "exchange"));

            System.out.println(
                    "Routing Keys: "
                            + deathEntry.get(
                            "routing-keys"));

        } catch (Exception e) {

            System.err.println(
                    "Failed to parse x-death header.");

            e.printStackTrace();
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

            System.err.println(
                    "Failed to close DLQ consumer.");

            e.printStackTrace();
        }
    }
}