package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ImageResizeTaskProcessorTest {

    private final ImageResizeTaskProcessor processor =
            new ImageResizeTaskProcessor();

    @Test
    @DisplayName(
            "Should process valid image resize task successfully")
    void process_ValidTask_Success()
            throws Exception {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "imageId",
                "img-101");

        payload.put(
                "width",
                1024);

        payload.put(
                "height",
                768);

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "resize_image",
                        payload,
                        Instant.now(),
                        0);

        assertDoesNotThrow(
                () -> processor.process(
                        task));
    }

    @Test
    @DisplayName(
            "Should throw exception when task is null")
    void process_NullTask_ThrowsException() {

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(
                        null));
    }

    @Test
    @DisplayName(
            "Should throw exception when payload is null")
    void process_NullPayload_ThrowsException() {

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "resize_image",
                        null,
                        Instant.now(),
                        0);

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(
                        task));
    }

    @Test
    @DisplayName(
            "Should throw exception when width is missing")
    void process_MissingWidth_ThrowsException() {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "imageId",
                "img-101");

        payload.put(
                "height",
                768);

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "resize_image",
                        payload,
                        Instant.now(),
                        0);

        assertThrows(
                Exception.class,
                () -> processor.process(
                        task));
    }

    @Test
    @DisplayName(
            "Should throw exception when height is missing")
    void process_MissingHeight_ThrowsException() {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "imageId",
                "img-101");

        payload.put(
                "width",
                1024);

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "resize_image",
                        payload,
                        Instant.now(),
                        0);

        assertThrows(
                Exception.class,
                () -> processor.process(
                        task));
    }
}