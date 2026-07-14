package com.sushobhit.taskqueue.dlq;

import com.sushobhit.taskqueue.common.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlqConsumerMain {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    DlqConsumerMain.class);

    public static void main(
            String[] args) {

        try {

            DlqConsumer dlqConsumer =
                    new DlqConsumer();

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                LOGGER.info(
                                        "Shutting down DLQ Consumer...");

                                dlqConsumer.close();

                                ConnectionManager.closeConnection();
                            }));

            dlqConsumer.start();

            LOGGER.info(
                    "DLQ Consumer is running...");

            while (true) {

                Thread.sleep(
                        1000);
            }

        } catch (Exception e) {

            LOGGER.error(
                    "Failed to start DLQ Consumer.",
                    e);
        }
    }
}