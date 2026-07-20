package com.sushobhit.taskqueue.common.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import java.util.UUID;


@Entity
@Table(name = "task_status")
public class TaskStatus {

    @Id
    @Column(columnDefinition = "UUID", updatable = false, nullable = false)
    private UUID taskId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Status status;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Column(length = 1024)
    private String resultMessage;

    /**
     * Possible task states.
     */
    public enum Status {
        ACCEPTED,
        PROCESSING,
        COMPLETED,
        FAILED
    }

    protected TaskStatus() {
    }

    public TaskStatus(UUID taskId) {
        this.taskId = taskId;
        this.status = Status.ACCEPTED;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = this.createdAt;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public Status getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public void setStatus(Status status) {
        this.status = status;
        this.updatedAt = LocalDateTime.now();
    }

    public void setResultMessage(String resultMessage) {
        this.resultMessage = resultMessage;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "TaskStatus{" +
                "taskId=" + taskId +
                ", status=" + status +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", resultMessage='" + resultMessage + '\'' +
                '}';
    }
}