package com.sushobhit.taskqueue.consumer;

import com.rabbitmq.client.Channel;

import com.sushobhit.taskqueue.consumer.processor.TaskProcessor;
import com.sushobhit.taskqueue.consumer.processor.TaskProcessorFactory;
import com.rabbitmq.client.Connection;
import com.sushobhit.taskqueue.common.ConnectionManager;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.nio.charset.StandardCharsets;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import com.sushobhit.taskqueue.message.TaskMessage;
import java.util.concurrent.TimeoutException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Consumer implements Runnable, AutoCloseable {

    private final String queueName;
    
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Consumer.class);
    
    private Connection connection;

    private Channel channel;

    private String consumerTag;
    
    private static final int MAX_RETRIES = 3;
    
    private static final long BASE_DELAY_MILLIS = 1000;

    private static final long MAX_DELAY_MILLIS = 30000;
   
    
    private static final String DLQ_NAME =
            "tasks_dlq";
    
    private static final String QUEUE_NAME_PREFIX =
            "task_queue_";
    
    private static final String EXCHANGE_NAME =
            "tasks_exchange";

    private static final String DLX_NAME =
            "tasks_dlx";
    
    private static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(
                            new JavaTimeModule());
    
    
private class TaskMessageHandler extends DefaultConsumer {

public TaskMessageHandler(
        Channel channel) {

    super(channel);
}

@Override
public void handleConsumeOk(
        String consumerTag) {

    Consumer.this.consumerTag =
            consumerTag;

    LOGGER.info(
            "Consumer registered successfully. Queue="
                    + Consumer.this.queueName
                    + ", ConsumerTag="
                    + consumerTag);
}

@Override
public void handleDelivery(
        String consumerTag,
        Envelope envelope,
        AMQP.BasicProperties properties,
        byte[] body)
        throws IOException {

    long deliveryTag =
            envelope.getDeliveryTag();

    String routingKey =
            envelope.getRoutingKey();
    String originalRoutingKey =
            routingKey;

    LOGGER.info(
            "Received message. DeliveryTag="
                    + deliveryTag
                    + ", RoutingKey="
                    + routingKey);

    TaskMessage taskMessage = null;
    String taskType = null;
    TaskProcessor processor = null;
    boolean processingSuccessful = false;

    String messageContent =
            "(unable to decode)";

    try {

        messageContent =
                new String(
                        body,
                        StandardCharsets.UTF_8);

        taskMessage =
                objectMapper.readValue(
                        messageContent,
                        TaskMessage.class);

        LOGGER.info(
                "TaskMessage received: "
                        + taskMessage);

        taskType =
                taskMessage.getTaskType();

        if (taskType == null
                || taskType.trim().isEmpty()) {

        	LOGGER.error(
                    "Task type is missing or empty.");

            return;
        }

        LOGGER.info(
                "Task type determined: "
                        + taskType);

        try {

            processor =
                    TaskProcessorFactory.getProcessor(
                            taskType);

            LOGGER.info(
                    "Processor selected: "
                            + processor.getClass()
                            .getSimpleName());

        } catch (IllegalArgumentException e) {

        	LOGGER.error(
                    "No processor found for task type: "
                            + taskType);

            return;
        }

    } catch (JsonProcessingException e) {

    	LOGGER.error(
                "Failed to deserialize TaskMessage.");

    	LOGGER.error(
                "Message Content: "
                        + messageContent);

        e.printStackTrace();

    } catch (Exception e) {

    	LOGGER.error(
                "Unexpected error while processing message.");

        e.printStackTrace();
    }
    if (taskMessage != null
            && processor != null) {

        try {

        	LOGGER.info(
                    "Executing processor: "
                            + processor.getClass()
                            .getSimpleName());

            processor.process(
                    taskMessage);

            processingSuccessful = true;

            LOGGER.info(
                    "Processor completed successfully. TaskId="
                            + taskMessage.getTaskId());

        } catch (Exception e) {

            processingSuccessful = false;

            LOGGER.error(
                    "Processor execution failed. TaskId="
                            + taskMessage.getTaskId());

            e.printStackTrace();
        }

    } else {

        processingSuccessful = false;

        LOGGER.error(
                "Skipping task processing due to previous errors.");
    }
    if (taskMessage == null) {

        try {

        	LOGGER.error(
                    "Message cannot be processed or retried. Sending to DLQ. DeliveryTag="
                            + deliveryTag);

            this.getChannel().basicNack(
                    deliveryTag,
                    false,
                    false);

            LOGGER.info(
                    "basicNack sent successfully. Message routed to DLQ.");

            return;

        } catch (IOException e) {

        	LOGGER.error(
                    "Failed to send basicNack. DeliveryTag="
                            + deliveryTag);

            e.printStackTrace();

            return;
        }
    }
    LOGGER.info(
            "Message processing completed. Success="
                    + processingSuccessful);
    
    try {

        if (processingSuccessful) {

        	LOGGER.info(
                    "Task processing successful. Sending ACK. DeliveryTag="
                            + deliveryTag);

            this.getChannel().basicAck(
                    deliveryTag,
                    false);

            LOGGER.info(
                    "ACK sent successfully. DeliveryTag="
                            + deliveryTag);

        } else {

        	if (taskMessage != null
        	        && taskMessage.getRetryCount()
        	        < MAX_RETRIES) {

        	    int nextRetryCount =
        	            taskMessage.getRetryCount() + 1;

        	    taskMessage.setRetryCount(
        	            nextRetryCount);

        	    LOGGER.info(
        	            "Retry attempt "
        	                    + nextRetryCount
        	                    + " of "
        	                    + MAX_RETRIES
        	                    + " for TaskId="
        	                    + taskMessage.getTaskId());

        	    long exponentialDelay =
        	            BASE_DELAY_MILLIS
        	                    * (long) Math.pow(
        	                    2,
        	                    taskMessage.getRetryCount() - 1);

        	    long jitter =
        	            (long) (
        	                    (Math.random() - 0.5)
        	                            * BASE_DELAY_MILLIS
        	                            * 0.4);

        	    long calculatedDelay =
        	            Math.min(
        	                    exponentialDelay + jitter,
        	                    MAX_DELAY_MILLIS);

        	    calculatedDelay =
        	            Math.max(
        	                    0,
        	                    calculatedDelay);

        	    LOGGER.info(
        	            "Waiting "
        	                    + calculatedDelay
        	                    + " ms before retry.");

        	    try {

        	        Thread.sleep(
        	                calculatedDelay);

        	    } catch (InterruptedException e) {

        	        Thread.currentThread()
        	                .interrupt();

        	        LOGGER.error(
        	                "Retry delay interrupted. Sending message to DLQ.");

        	        this.getChannel().basicNack(
        	                deliveryTag,
        	                false,
        	                false);

        	        return;
        	    }

        	    Channel publishChannel = null;

        	    try {

        	        publishChannel =
        	                ConnectionManager
        	                        .getConnection()
        	                        .createChannel();

        	        byte[] retryBody =
        	                objectMapper.writeValueAsBytes(
        	                        taskMessage);

        	        AMQP.BasicProperties retryProperties =
        	                MessageProperties
        	                        .PERSISTENT_TEXT_PLAIN
        	                        .builder()
        	                        .contentType(
        	                                "application/json")
        	                        .deliveryMode(2)
        	                        .build();

        	        publishChannel.basicPublish(
        	                EXCHANGE_NAME,
        	                originalRoutingKey,
        	                retryProperties,
        	                retryBody);

        	        this.getChannel().basicAck(
        	                deliveryTag,
        	                false);

        	        LOGGER.info(
        	                "Task republished successfully. Original message ACKed.");

        	    } catch (IOException | TimeoutException e) {

        	    	LOGGER.error(
        	                "Failed to republish retry message.");

        	        e.printStackTrace();

        	    } finally {

        	        if (publishChannel != null
        	                && publishChannel.isOpen()) {

        	            try {

        	                publishChannel.close();

        	            } catch (Exception e) {

        	            	LOGGER.error(
        	                        "Failed to close retry publish channel.");

        	                e.printStackTrace();
        	            }
        	        }
        	    }

        	} else {

        		LOGGER.error(
        	            "Maximum retry limit reached. Sending message to DLQ. DeliveryTag="
        	                    + deliveryTag);

        	    this.getChannel().basicNack(
        	            deliveryTag,
        	            false,
        	            false);

        	    LOGGER.info(
        	            "basicNack sent successfully. Message routed to DLQ.");
        	}
        }

    } catch (IOException e) {

    	LOGGER.error(
                "Failed to send ACK. DeliveryTag="
                        + deliveryTag);

        e.printStackTrace();
    }
}

@Override
public void handleShutdownSignal(
        String consumerTag,
        ShutdownSignalException sig) {

    if (!sig.isInitiatedByApplication()) {

    	LOGGER.error(
                "Consumer shutdown unexpectedly: "
                        + sig.getMessage());

    } else {

    	LOGGER.info(
                "Consumer shutdown by application.");
    }
}
}
    

    public Consumer(String queueName) {

        if (queueName == null
                || queueName.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "Queue name cannot be null or empty.");
        }

        this.queueName = queueName;

        LOGGER.info(
                "Initializing Consumer for queue: "
                        + this.queueName);
    }

    private void setupConnectionAndChannel()
            throws IOException, TimeoutException {

        ConnectionManager.initialize();

        this.connection =
                ConnectionManager.getConnection();

        LOGGER.info(
                "Consumer obtained connection: "
                        + this.connection);

        this.channel =
                this.connection.createChannel();

        LOGGER.info(
                "Consumer created channel: "
                        + this.channel);

        this.channel.addShutdownListener(reason -> {

            if (!reason.isInitiatedByApplication()) {

            	LOGGER.error(
                        "Consumer channel closed unexpectedly: "
                                + reason.getMessage());

            } else {

            	LOGGER.info(
                        "Consumer channel closed by application.");
            }
        });
    }

    public void initializeAndStart()
            throws IOException, TimeoutException {

    	LOGGER.info(
                "Consumer initialization started for queue: "
                        + this.queueName);

        setupConnectionAndChannel();

        setupTopology();

        startConsuming();
    }

    private void setupTopology()
            throws IOException {

        if (this.channel == null
                || !this.channel.isOpen()) {

            throw new IOException(
                    "Cannot setup topology: Channel is not available.");
        }

        channel.exchangeDeclare(
                EXCHANGE_NAME,
                BuiltinExchangeType.DIRECT,
                true
        );

        channel.exchangeDeclare(
                DLX_NAME,
                BuiltinExchangeType.FANOUT,
                true
        );

        Map<String, Object> queueArguments =
                new HashMap<>();

        queueArguments.put(
                "x-dead-letter-exchange",
                DLX_NAME);

        queueArguments.put(
                "x-dead-letter-routing-key",
                this.queueName);

        AMQP.Queue.DeclareOk declareOk =
                channel.queueDeclare(
                        this.queueName,
                        true,
                        false,
                        false,
                        queueArguments
                );
        String routingKey =
                deriveRoutingKeyFromQueue(
                        this.queueName);
        

        if (routingKey == null) {

            throw new IOException(
                    "Unable to derive routing key for queue: "
                            + this.queueName);
        }

        channel.queueBind(
                this.queueName,
                EXCHANGE_NAME,
                routingKey
        );
        AMQP.Queue.DeclareOk deadLetterQueue =
                channel.queueDeclare(
                        DLQ_NAME,
                        true,
                        false,
                        false,
                        null
                );

        LOGGER.info(
                "Dead-letter queue declared: "
                        + deadLetterQueue.getQueue());

        channel.queueBind(
                DLQ_NAME,
                DLX_NAME,
                "#"
        );

        LOGGER.info(
                "Dead-letter queue bound successfully. DLQ="
                        + DLQ_NAME
                        + ", DLX="
                        + DLX_NAME);

        LOGGER.info(
                "Queue bound successfully. Queue="
                        + this.queueName
                        + ", RoutingKey="
                        + routingKey);

        LOGGER.info(
                "Queue declared successfully: "
                        + declareOk.getQueue());
    }

    private void startConsuming()
            throws IOException {

        if (this.channel == null
                || !this.channel.isOpen()) {

            throw new IOException(
                    "Cannot start consuming: Channel is not available.");
        }

        LOGGER.info(
                "Starting consumer on queue: "
                        + this.queueName);

        DefaultConsumer messageHandler =
                new TaskMessageHandler(
                        this.channel);

        try {
        	channel.basicQos(1);
        	LOGGER.info("Prefetch count set to 1.");
            channel.basicConsume(
                    this.queueName,
                    false,
                    "",
                    messageHandler
            );

            LOGGER.info(
                    "basicConsume registered successfully for queue: "
                            + this.queueName);

        } catch (IOException e) {

        	LOGGER.error(
                    "Failed to start consumer for queue: "
                            + this.queueName);

            throw e;
        }
    }

    @Override
    public void run() {

        try {

            initializeAndStart();

            while (channel != null
                    && channel.isOpen()) {

                Thread.sleep(10000);
            }

        } catch (IOException
                 | TimeoutException
                 | InterruptedException e) {

        	LOGGER.error(
                    "Consumer error: "
                            + e.getMessage());

            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {

        try {

            if (this.channel != null
                    && this.channel.isOpen()) {

                if (this.consumerTag != null) {

                    channel.basicCancel(
                            this.consumerTag);
                }

                channel.close();
            }

        } catch (Exception e) {

        	LOGGER.error(
                    "Error closing consumer: "
                            + e.getMessage());
        } finally {

            this.channel = null;
            this.connection = null;
        }
    }
    
    private String deriveRoutingKeyFromQueue(
            String queueName) {

        if (queueName == null
                || !queueName.startsWith(
                QUEUE_NAME_PREFIX)) {

            return null;
        }

        if (queueName.equals(
                "task_queue_email")) {

            return "send_email";
        }

        if (queueName.equals(
                "task_queue_image_processing")) {

            return "resize_image";
        }

        return queueName.substring(
                QUEUE_NAME_PREFIX.length());
    }
}