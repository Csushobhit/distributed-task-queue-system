package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;

import java.util.Map;

public class EmailTaskProcessor implements TaskProcessor {

    @Override
    public void process(TaskMessage task)
            throws Exception {

        if (task == null) {

            throw new IllegalArgumentException(
                    "TaskMessage cannot be null.");
        }

        Map<String, Object> payload =
                task.getPayload();

        if (payload == null) {

            throw new IllegalArgumentException(
                    "Payload cannot be null.");
        }

        try {

            String recipient =
                    String.valueOf(
                            payload.get("recipient"));

            String subject =
                    String.valueOf(
                            payload.get("subject"));

            String body =
                    String.valueOf(
                            payload.get("body"));

            System.out.println(
                    "Processing email task. TaskId="
                            + task.getTaskId());

            System.out.println(
                    "Recipient="
                            + recipient);

            System.out.println(
                    "Subject="
                            + subject);

            System.out.println(
                    "Body Length="
                            + body.length());

            Thread.sleep(500);

            System.out.println(
                    "Email task completed successfully. TaskId="
                            + task.getTaskId());

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new Exception(
                    "Email task interrupted.",
                    e);
        }
    }
}