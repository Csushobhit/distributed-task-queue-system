package com.sushobhit.taskqueue.producer.controller;

import com.sushobhit.taskqueue.message.TaskMessage;
import com.sushobhit.taskqueue.producer.Producer;
import com.sushobhit.taskqueue.producer.dto.TaskSubmissionRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    TaskController.class);

    private final Producer taskProducer;

    @Autowired
    public TaskController(
            Producer taskProducer) {

        this.taskProducer =
                taskProducer;

        LOGGER.info(
                "TaskController initialized with Producer instance.");
    }

    @PostMapping
    public ResponseEntity<Map<String, String>>
    submitTask(
            @RequestBody
            TaskSubmissionRequest taskRequest) {

        LOGGER.info(
                "Received task submission request: {}",
                taskRequest);

        if (taskRequest == null
                || taskRequest.getTaskType() == null
                || taskRequest.getTaskType().isBlank()) {

            LOGGER.warn(
                    "Received invalid task submission: missing task type.");

            return ResponseEntity
                    .badRequest()
                    .body(
                            Map.of(
                                    "error",
                                    "Task type must be provided."));
        }

        if (taskRequest.getPayload() == null) {

            LOGGER.warn(
                    "Received invalid task submission: missing payload.");

            return ResponseEntity
                    .badRequest()
                    .body(
                            Map.of(
                                    "error",
                                    "Task payload must be provided."));
        }

        UUID taskId =
                UUID.randomUUID();

        LOGGER.debug(
                "Generated Task ID: {}",
                taskId);

        TaskMessage taskMessage =
                new TaskMessage(
                        taskId,
                        taskRequest.getTaskType(),
                        taskRequest.getPayload(),
                        Instant.now(),
                        0);

        LOGGER.debug(
                "Created TaskMessage object: {}",
                taskMessage);

        try {

            taskProducer.sendTask(
                    taskMessage);

            LOGGER.info(
                    "Successfully submitted task [{}] to producer.",
                    taskMessage.getTaskId());

            Map<String, String> response =
                    Map.of(
                            "taskId",
                            taskMessage.getTaskId().toString(),
                            "status",
                            "accepted");

            return ResponseEntity
                    .accepted()
                    .body(
                            response);

        } catch (IOException | RuntimeException e) {

            LOGGER.error(
                    "Failed to send task [{}] to producer.",
                    taskMessage.getTaskId(),
                    e);

            return ResponseEntity
                    .status(
                            HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            Map.of(
                                    "error",
                                    "Failed to enqueue task. Please check server logs or try again later."));
        }
    }
}