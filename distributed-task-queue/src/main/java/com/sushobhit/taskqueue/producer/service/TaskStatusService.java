package com.sushobhit.taskqueue.producer.service;

import com.sushobhit.taskqueue.common.model.TaskStatus;
import com.sushobhit.taskqueue.common.repository.TaskStatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class TaskStatusService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusService.class);

    private final TaskStatusRepository taskStatusRepository;

    public TaskStatusService(TaskStatusRepository taskStatusRepository) {
        this.taskStatusRepository = taskStatusRepository;
    }


    @Transactional(readOnly = true)
    public Optional<TaskStatus> getTaskStatus(UUID taskId) {
        LOGGER.debug("Querying status for Task ID: {}", taskId);

        Optional<TaskStatus> taskStatus = taskStatusRepository.findById(taskId);

        if (taskStatus.isPresent()) {
            LOGGER.debug("Status found for Task ID {}: {}", taskId, taskStatus.get().getStatus());
        } else {
            LOGGER.debug("No status found for Task ID: {}", taskId);
        }

        return taskStatus;
    }

    @Transactional
    public TaskStatus createTaskRecord(UUID taskId) {

        LOGGER.info("Creating initial status record for Task ID: {}", taskId);

        if (taskStatusRepository.existsById(taskId)) {
            LOGGER.warn("Task status already exists for Task ID: {}", taskId);
            return taskStatusRepository.findById(taskId).orElseThrow();
        }

        TaskStatus taskStatus = new TaskStatus(taskId);

        return taskStatusRepository.save(taskStatus);
    }
    
    @Transactional
    public void markProcessing(UUID taskId) {

        TaskStatus status =
                taskStatusRepository.findById(taskId)
                        .orElseThrow();

        status.setStatus(TaskStatus.Status.PROCESSING);

        taskStatusRepository.save(status);

        LOGGER.info(
                "Task [{}] marked PROCESSING.",
                taskId);
    }

    @Transactional
    public void markCompleted(
            UUID taskId,
            String resultMessage) {

        TaskStatus status =
                taskStatusRepository.findById(taskId)
                        .orElseThrow();

        status.setStatus(TaskStatus.Status.COMPLETED);
        status.setResultMessage(resultMessage);

        taskStatusRepository.save(status);

        LOGGER.info(
                "Task [{}] marked COMPLETED.",
                taskId);
    }

    @Transactional
    public void markFailed(
            UUID taskId,
            String resultMessage) {

        TaskStatus status =
                taskStatusRepository.findById(taskId)
                        .orElseThrow();

        status.setStatus(TaskStatus.Status.FAILED);
        status.setResultMessage(resultMessage);

        taskStatusRepository.save(status);

        LOGGER.info(
                "Task [{}] marked FAILED.",
                taskId);
    }
}