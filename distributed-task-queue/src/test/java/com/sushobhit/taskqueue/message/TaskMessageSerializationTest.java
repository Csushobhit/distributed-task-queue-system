package com.sushobhit.taskqueue.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TaskMessageSerializationTest {

    private ObjectMapper objectMapper;

    private UUID sampleTaskId;

    private Instant sampleTimestamp;

    private Map<String, Object> samplePayload;

    private TaskMessage sampleTaskMessage;

    @BeforeEach
    void setUp() {

        objectMapper =
                new ObjectMapper();

        objectMapper.registerModule(
                new JavaTimeModule());

        sampleTaskId =
                UUID.fromString(
                        "a1b2c3d4-e5f6-7890-1234-567890abcdef");

        sampleTimestamp =
                Instant.parse(
                        "2026-07-15T10:30:00Z");

        samplePayload =
                new HashMap<>();

        samplePayload.put(
                "recipient",
                "test@example.com");

        samplePayload.put(
                "retryAttempts",
                3);

        samplePayload.put(
                "active",
                true);

        sampleTaskMessage =
                new TaskMessage(
                        sampleTaskId,
                        "EMAIL_TASK",
                        samplePayload,
                        sampleTimestamp,
                        0);
    }

    @Test
    @DisplayName(
            "Should serialize TaskMessage to JSON")
    void testSerialization()
            throws JsonProcessingException {

        String jsonOutput =
                objectMapper.writeValueAsString(
                        sampleTaskMessage);

        assertThat(
                jsonOutput)
                .isNotNull();

        assertThat(
                jsonOutput)
                .contains(
                        "\"taskId\":\""
                                + sampleTaskId
                                + "\"");

        assertThat(
                jsonOutput)
                .contains(
                        "\"taskType\":\"EMAIL_TASK\"");

        assertThat(
                jsonOutput)
                .contains(
                        "\"recipient\":\"test@example.com\"");

        assertThat(
                jsonOutput)
                .contains(
                        "\"retryAttempts\":3");

        assertThat(
                jsonOutput)
                .contains(
                        "\"active\":true");

        assertThat(
                jsonOutput)
                .contains(
                        "\"retryCount\":0");
    }

    @Test
    @DisplayName(
            "Should deserialize JSON into TaskMessage")
    void testDeserialization()
            throws JsonProcessingException {

        String jsonInput =
                "{"
                        + "\"taskId\":\"a1b2c3d4-e5f6-7890-1234-567890abcdef\","
                        + "\"taskType\":\"EMAIL_TASK\","
                        + "\"payload\":{"
                        + "\"recipient\":\"test@example.com\","
                        + "\"retryAttempts\":3,"
                        + "\"active\":true"
                        + "},"
                        + "\"timestamp\":\"2026-07-15T10:30:00Z\","
                        + "\"retryCount\":0"
                        + "}";

        TaskMessage deserializedTask =
                objectMapper.readValue(
                        jsonInput,
                        TaskMessage.class);

        assertThat(
                deserializedTask)
                .isNotNull();

        assertThat(
                deserializedTask.getTaskId())
                .isEqualTo(
                        sampleTaskId);

        assertThat(
                deserializedTask.getTaskType())
                .isEqualTo(
                        "EMAIL_TASK");

        assertThat(
                deserializedTask.getTimestamp())
                .isEqualTo(
                        sampleTimestamp);

        assertThat(
                deserializedTask.getRetryCount())
                .isEqualTo(
                        0);

        assertThat(
                deserializedTask.getPayload())
                .isNotNull();

        assertThat(
                deserializedTask.getPayload()
                        .get(
                                "recipient"))
                .isEqualTo(
                        "test@example.com");

        assertThat(
                deserializedTask.getPayload()
                        .get(
                                "retryAttempts"))
                .isEqualTo(
                        3);

        assertThat(
                deserializedTask.getPayload()
                        .get(
                                "active"))
                .isEqualTo(
                        true);
    }

    @Test
    @DisplayName(
            "Should support round trip serialization")
    void testRoundTrip()
            throws JsonProcessingException {

        String json =
                objectMapper.writeValueAsString(
                        sampleTaskMessage);

        TaskMessage roundTrippedTask =
                objectMapper.readValue(
                        json,
                        TaskMessage.class);

        assertThat(
                roundTrippedTask)
                .isEqualTo(
                        sampleTaskMessage);
    }

    @Test
    @DisplayName(
            "Should handle null payload")
    void testNullPayload()
            throws JsonProcessingException {

        TaskMessage taskWithNullPayload =
                new TaskMessage(
                        UUID.randomUUID(),
                        "SYSTEM_TASK",
                        null,
                        Instant.now(),
                        0);

        String json =
                objectMapper.writeValueAsString(
                        taskWithNullPayload);

        assertThat(
                json)
                .contains(
                        "\"payload\":null");

        TaskMessage deserializedTask =
                objectMapper.readValue(
                        json,
                        TaskMessage.class);

        assertThat(
                deserializedTask.getPayload())
                .isNull();
    }

    @Test
    @DisplayName(
            "Should handle empty payload")
    void testEmptyPayload()
            throws JsonProcessingException {

        TaskMessage taskWithEmptyPayload =
                new TaskMessage(
                        UUID.randomUUID(),
                        "ANOTHER_TASK",
                        new HashMap<>(),
                        Instant.now(),
                        1);

        String json =
                objectMapper.writeValueAsString(
                        taskWithEmptyPayload);

        assertThat(
                json)
                .contains(
                        "\"payload\":{}");

        TaskMessage deserializedTask =
                objectMapper.readValue(
                        json,
                        TaskMessage.class);

        assertThat(
                deserializedTask.getPayload())
                .isNotNull()
                .isEmpty();

        assertThat(
                deserializedTask.getRetryCount())
                .isEqualTo(
                        1);
    }
}