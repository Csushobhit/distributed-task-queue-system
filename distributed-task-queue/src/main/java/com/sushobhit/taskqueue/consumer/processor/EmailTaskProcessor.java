package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EmailTaskProcessor implements TaskProcessor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    EmailTaskProcessor.class);

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

            if (recipient == null
                    || recipient.isBlank()
                    || "null".equalsIgnoreCase(recipient)
                    || !recipient.contains("@")) {

                throw new IllegalArgumentException(
                        "Invalid recipient email: "
                                + recipient);
            }

            if (subject == null
                    || subject.isBlank()
                    || "null".equalsIgnoreCase(subject)) {

                throw new IllegalArgumentException(
                        "Email subject cannot be empty.");
            }

            if (body == null
                    || body.isBlank()
                    || "null".equalsIgnoreCase(body)) {

                throw new IllegalArgumentException(
                        "Email body cannot be empty.");
            }

            LOGGER.info(
                    "Processing email task. TaskId={}",
                    task.getTaskId());

            LOGGER.info(
                    "Recipient={}",
                    recipient);

            LOGGER.info(
                    "Subject={}",
                    subject);

            LOGGER.info(
                    "Body Length={}",
                    body.length());

            Thread.sleep(500);

            LOGGER.info(
                    "Email task completed successfully. TaskId={}",
                    task.getTaskId());

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new Exception(
                    "Email task interrupted.",
                    e);
        }
    }
}