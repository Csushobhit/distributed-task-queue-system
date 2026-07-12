package com.sushobhit.taskqueue.main;

import com.sushobhit.taskqueue.common.ConnectionManager;
import com.sushobhit.taskqueue.message.TaskMessage;
import com.sushobhit.taskqueue.producer.Producer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class ProducerMain {

    public static void main(String[] args) {

        System.out.println("Starting Producer Application...");

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

            System.out.println(
                    "Tasks submitted. Waiting for publisher confirms..."
            );

            Thread.sleep(5000);

            System.out.println(
                    "Finished waiting for publisher confirms."
            );

        } catch (Exception e) {

            System.err.println(
                    "Producer application failed: "
                            + e.getMessage()
            );

            e.printStackTrace();

        } finally {

            ConnectionManager.closeConnection();
        }

        System.out.println(
                "Producer Application Finished."
        );
    }
}