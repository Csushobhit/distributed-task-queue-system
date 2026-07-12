package com.sushobhit.taskqueue.dlq;

import com.sushobhit.taskqueue.common.ConnectionManager;

public class DlqConsumerMain {

    public static void main(
            String[] args) {

        try {

            DlqConsumer dlqConsumer =
                    new DlqConsumer();

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                System.out.println(
                                        "Shutting down DLQ Consumer...");

                                dlqConsumer.close();

                                ConnectionManager.closeConnection();
                            }));

            dlqConsumer.start();

            System.out.println(
                    "DLQ Consumer is running...");

            while (true) {

                Thread.sleep(
                        1000);
            }

        } catch (Exception e) {

            System.err.println(
                    "Failed to start DLQ Consumer.");

            e.printStackTrace();
        }
    }
}