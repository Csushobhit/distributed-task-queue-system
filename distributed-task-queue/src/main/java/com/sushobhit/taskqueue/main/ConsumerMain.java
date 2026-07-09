package com.sushobhit.taskqueue.main;

import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.consumer.Consumer;

public class ConsumerMain {

    private static final String QUEUE_NAME =
            "task_queue_email";

    public static void main(String[] args) {

        System.out.println(
                "Starting Consumer Application...");

        Consumer consumer = null;

        try {

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

            System.out.println(
                    "Consumer thread started for queue: "
                            + QUEUE_NAME);

            Consumer finalConsumer =
                    consumer;

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                System.out.println(
                                        "Shutdown hook triggered.");

                                try {

                                    if (finalConsumer != null) {

                                        finalConsumer.close();
                                    }

                                } catch (Exception e) {

                                    System.err.println(
                                            "Error while closing consumer: "
                                                    + e.getMessage());
                                }

                                ConnectionManager.closeConnection();

                                System.out.println(
                                        "Consumer application shutdown complete.");
                            }));

            System.out.println(
                    "Consumer is waiting for messages...");

        } catch (Exception e) {

            System.err.println(
                    "Failed to start consumer application.");

            e.printStackTrace();

            ConnectionManager.closeConnection();
        }
    }
}