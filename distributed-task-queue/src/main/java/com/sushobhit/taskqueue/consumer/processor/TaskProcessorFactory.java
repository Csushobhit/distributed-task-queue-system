package com.sushobhit.taskqueue.consumer.processor;

public final class TaskProcessorFactory {

    private TaskProcessorFactory() {
    }

    public static TaskProcessor getProcessor(
            String taskType) {

        if (taskType == null
                || taskType.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "Task type cannot be null or empty.");
        }

        switch (taskType) {

            case "send_email":

                return new EmailTaskProcessor();

            case "resize_image":

                return new ImageResizeTaskProcessor();

            default:

                throw new IllegalArgumentException(
                        "No processor available for task type: "
                                + taskType);
        }
    }
}