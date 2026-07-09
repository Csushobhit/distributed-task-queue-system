package com.sushobhit.taskqueue.message;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class TaskMessage implements Serializable {

    private UUID taskId;
    private String taskType;
    private Map<String, Object> payload;
    private Instant timestamp;
    private int retryCount;

    public TaskMessage() {
        this.retryCount = 0;
    }

    public TaskMessage(UUID taskId,
                       String taskType,
                       Map<String, Object> payload,
                       Instant timestamp,
                       int retryCount) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.payload = payload;
        this.timestamp = timestamp;
        this.retryCount = retryCount;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public void setTaskId(UUID taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    @Override
    public String toString() {
        return "TaskMessage{" +
                "taskId=" + taskId +
                ", taskType='" + Objects.toString(taskType, "null") + '\'' +
                ", payload=" + Objects.toString(payload, "null") +
                ", timestamp=" + Objects.toString(timestamp, "null") +
                ", retryCount=" + retryCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskMessage that = (TaskMessage) o;

        return retryCount == that.retryCount &&
                Objects.equals(taskId, that.taskId) &&
                Objects.equals(taskType, that.taskType) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, taskType, payload, timestamp, retryCount);
    }
}