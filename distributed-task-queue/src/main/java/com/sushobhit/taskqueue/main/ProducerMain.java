package com.sushobhit.taskqueue.main;

import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.common.MetricsManager;
import com.sushobhit.taskqueue.message.TaskMessage;
import com.sushobhit.taskqueue.producer.Producer;

import io.prometheus.client.exporter.HTTPServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class ProducerMain {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    ProducerMain.class);

    public static void main(String[] args) {

        HTTPServer metricsServer = null;

        LOGGER.info(
                "Starting Producer Application...");

        try {

            metricsServer =
                    new HTTPServer(
                            new InetSocketAddress(
                                    8080),
                            MetricsManager
                                    .getPrometheusRegistry()
                                    .getPrometheusRegistry(),
                            true);

            LOGGER.info(
                    "Prometheus metrics endpoint started on port 8080.");

            try (Producer producer = new Producer()) {

                TaskMessage emailTask = new TaskMessage();

                emailTask.setTaskId(UUID.randomUUID());
                emailTask.setTaskType("send_email");
                emailTask.setPayload(
                        Map.of(
                                "recipient", "test@user.com",
                                "subject", "Welcome",
                                "body", "Hello from RabbitMQ Producer"
                        )
                );
                emailTask.setTimestamp(Instant.now());

                producer.sendTask(emailTask);

                TaskMessage imageTask = new TaskMessage();

                imageTask.setTaskId(UUID.randomUUID());
                imageTask.setTaskType("resize_image");
                imageTask.setPayload(
                        Map.of(
                                "imageId", "img-101",
                                "width", 1024,
                                "height", 768
                        )
                );
                imageTask.setTimestamp(Instant.now());

                producer.sendTask(imageTask);

                LOGGER.info(
                        "Tasks submitted. Waiting for publisher confirms...");

                Thread.sleep(5000);

                LOGGER.info(
                        "Finished waiting for publisher confirms.");
            }

        } catch (Exception e) {

            LOGGER.error(
                    "Producer application failed.",
                    e);

        } finally {

            if (metricsServer != null) {

                metricsServer.stop();
            }

            ConnectionManager.closeConnection();
        }

        LOGGER.info(
                "Producer Application Finished.");
    }
}