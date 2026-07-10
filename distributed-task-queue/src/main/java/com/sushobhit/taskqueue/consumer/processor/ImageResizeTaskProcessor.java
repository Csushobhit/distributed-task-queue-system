package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;

import java.util.Map;

public class ImageResizeTaskProcessor
        implements TaskProcessor {

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

            String imageId =
                    String.valueOf(
                            payload.get("imageId"));

            Number width =
                    (Number) payload.get("width");

            Number height =
                    (Number) payload.get("height");

            System.out.println(
                    "Processing image resize task. TaskId="
                            + task.getTaskId());

            System.out.println(
                    "ImageId="
                            + imageId);

            System.out.println(
                    "Target Size="
                            + width.intValue()
                            + "x"
                            + height.intValue());

            Thread.sleep(1000);

            System.out.println(
                    "Image resize task completed successfully. TaskId="
                            + task.getTaskId());

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new Exception(
                    "Image resize task interrupted.",
                    e);
        }
    }
}