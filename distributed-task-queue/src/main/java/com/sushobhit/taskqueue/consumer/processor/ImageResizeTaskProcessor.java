package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ImageResizeTaskProcessor
        implements TaskProcessor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    ImageResizeTaskProcessor.class);

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

            LOGGER.info(
                    "Processing image resize task. TaskId={}",
                    task.getTaskId());

            LOGGER.info(
                    "ImageId={}",
                    imageId);

            LOGGER.info(
                    "Target Size={}x{}",
                    width.intValue(),
                    height.intValue());

            Thread.sleep(1000);

            LOGGER.info(
                    "Image resize task completed successfully. TaskId={}",
                    task.getTaskId());

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new Exception(
                    "Image resize task interrupted.",
                    e);
        }
    }
}