package com.sushobhit.taskqueue.main;

import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.common.MetricsManager;
import com.sushobhit.taskqueue.consumer.Consumer;

import io.prometheus.client.exporter.HTTPServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class ConsumerMain {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    ConsumerMain.class);

    private static final String QUEUE_NAME =
            "task_queue_email";

    public static void main(String[] args) {

        HTTPServer metricsServer = null;

        LOGGER.info(
                "Starting Consumer Application...");

        Consumer consumer = null;

        try {

            metricsServer =
                    new HTTPServer(
                            new InetSocketAddress(
                                    8081),
                            MetricsManager
                                    .getPrometheusRegistry()
                                    .getPrometheusRegistry(),
                            true);

            LOGGER.info(
                    "Prometheus metrics endpoint started on port 8081.");

            consumer =
                    new Consumer(
                            QUEUE_NAME);

            Thread consumerThread =
                    new Thread(
                            consumer);

            consumerThread.setName(
                    "ConsumerThread-"
                            + QUEUE_NAME);

            consumerThread.start();

            LOGGER.info(
                    "Consumer thread started for queue: {}",
                    QUEUE_NAME);

            Consumer finalConsumer =
                    consumer;
            HTTPServer finalMetricsServer =
                    metricsServer;

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                LOGGER.info(
                                        "Shutdown hook triggered.");

                                try {

                                    if (finalConsumer != null) {

                                        finalConsumer.close();
                                    }

                                } catch (Exception e) {

                                    LOGGER.error(
                                            "Error while closing consumer.",
                                            e);
                                }

                                if (finalMetricsServer != null) {

                                    finalMetricsServer.stop();
                                }

                                ConnectionManager.closeConnection();

                                LOGGER.info(
                                        "Consumer application shutdown complete.");
                            }));

            LOGGER.info(
                    "Consumer is waiting for messages...");

        } catch (Exception e) {

            LOGGER.error(
                    "Failed to start consumer application.",
                    e);

            if (metricsServer != null) {

                metricsServer.stop();
            }

            ConnectionManager.closeConnection();
        }
    }
}