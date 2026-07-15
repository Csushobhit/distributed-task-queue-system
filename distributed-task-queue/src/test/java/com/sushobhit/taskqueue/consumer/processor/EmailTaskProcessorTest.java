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

class EmailTaskProcessorTest {

    private final EmailTaskProcessor processor =
            new EmailTaskProcessor();

    @Test
    @DisplayName(
            "Should process valid email task successfully")
    void process_ValidTask_Success()
            throws Exception {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "recipient",
                "test@example.com");

        payload.put(
                "subject",
                "Welcome");

        payload.put(
                "body",
                "Hello User");

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "send_email",
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
                        "send_email",
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
            "Should throw exception for invalid recipient")
    void process_InvalidRecipient_ThrowsException() {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "recipient",
                "invalid-email");

        payload.put(
                "subject",
                "Welcome");

        payload.put(
                "body",
                "Hello User");

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "send_email",
                        payload,
                        Instant.now(),
                        0);

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(
                        task));
    }

    @Test
    @DisplayName(
            "Should throw exception for blank subject")
    void process_BlankSubject_ThrowsException() {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "recipient",
                "test@example.com");

        payload.put(
                "subject",
                "");

        payload.put(
                "body",
                "Hello User");

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "send_email",
                        payload,
                        Instant.now(),
                        0);

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(
                        task));
    }

    @Test
    @DisplayName(
            "Should throw exception for blank body")
    void process_BlankBody_ThrowsException() {

        Map<String, Object> payload =
                new HashMap<>();

        payload.put(
                "recipient",
                "test@example.com");

        payload.put(
                "subject",
                "Welcome");

        payload.put(
                "body",
                "");

        TaskMessage task =
                new TaskMessage(
                        UUID.randomUUID(),
                        "send_email",
                        payload,
                        Instant.now(),
                        0);

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(
                        task));
    }
}