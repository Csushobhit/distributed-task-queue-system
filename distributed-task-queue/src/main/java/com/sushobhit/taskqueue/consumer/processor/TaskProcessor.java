package com.sushobhit.taskqueue.consumer.processor;

import com.sushobhit.taskqueue.message.TaskMessage;

@FunctionalInterface
public interface TaskProcessor {

    void process(TaskMessage task)
            throws Exception;
}