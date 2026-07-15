package com.sushobhit.taskqueue.producer.dto;

import java.util.Map;

public class TaskSubmissionRequest {

    private String taskType;

    private Map<String, Object> payload;

    public String getTaskType() {

        return taskType;
    }

    public void setTaskType(
            String taskType) {

        this.taskType = taskType;
    }

    public Map<String, Object> getPayload() {

        return payload;
    }

    public void setPayload(
            Map<String, Object> payload) {

        this.payload = payload;
    }

    @Override
    public String toString() {

        return "TaskSubmissionRequest{" +
                "taskType='" + taskType + '\'' +
                ", payload=" + payload +
                '}';
    }
}