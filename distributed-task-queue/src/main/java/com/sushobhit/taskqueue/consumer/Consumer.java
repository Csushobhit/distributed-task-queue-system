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

public class Consumer implements Runnable, AutoCloseable {

    private final String queueName;

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

    System.out.println(
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

    System.out.println(
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

        System.out.println(
                "TaskMessage received: "
                        + taskMessage);

        taskType =
                taskMessage.getTaskType();

        if (taskType == null
                || taskType.trim().isEmpty()) {

            System.err.println(
                    "Task type is missing or empty.");

            return;
        }

        System.out.println(
                "Task type determined: "
                        + taskType);

        try {

            processor =
                    TaskProcessorFactory.getProcessor(
                            taskType);

            System.out.println(
                    "Processor selected: "
                            + processor.getClass()
                            .getSimpleName());

        } catch (IllegalArgumentException e) {

            System.err.println(
                    "No processor found for task type: "
                            + taskType);

            return;
        }

    } catch (JsonProcessingException e) {

        System.err.println(
                "Failed to deserialize TaskMessage.");

        System.err.println(
                "Message Content: "
                        + messageContent);

        e.printStackTrace();

    } catch (Exception e) {

        System.err.println(
                "Unexpected error while processing message.");

        e.printStackTrace();
    }
    if (taskMessage != null
            && processor != null) {

        try {

            System.out.println(
                    "Executing processor: "
                            + processor.getClass()
                            .getSimpleName());

            processor.process(
                    taskMessage);

            processingSuccessful = true;

            System.out.println(
                    "Processor completed successfully. TaskId="
                            + taskMessage.getTaskId());

        } catch (Exception e) {

            processingSuccessful = false;

            System.err.println(
                    "Processor execution failed. TaskId="
                            + taskMessage.getTaskId());

            e.printStackTrace();
        }

    } else {

        processingSuccessful = false;

        System.err.println(
                "Skipping task processing due to previous errors.");
    }
    if (taskMessage == null) {

        try {

            System.err.println(
                    "Message cannot be processed or retried. Sending to DLQ. DeliveryTag="
                            + deliveryTag);

            this.getChannel().basicNack(
                    deliveryTag,
                    false,
                    false);

            System.out.println(
                    "basicNack sent successfully. Message routed to DLQ.");

            return;

        } catch (IOException e) {

            System.err.println(
                    "Failed to send basicNack. DeliveryTag="
                            + deliveryTag);

            e.printStackTrace();

            return;
        }
    }
    System.out.println(
            "Message processing completed. Success="
                    + processingSuccessful);
    
    try {

        if (processingSuccessful) {

            System.out.println(
                    "Task processing successful. Sending ACK. DeliveryTag="
                            + deliveryTag);

            this.getChannel().basicAck(
                    deliveryTag,
                    false);

            System.out.println(
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

        	    System.out.println(
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

        	    System.out.println(
        	            "Waiting "
        	                    + calculatedDelay
        	                    + " ms before retry.");

        	    try {

        	        Thread.sleep(
        	                calculatedDelay);

        	    } catch (InterruptedException e) {

        	        Thread.currentThread()
        	                .interrupt();

        	        System.err.println(
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

        	        System.out.println(
        	                "Task republished successfully. Original message ACKed.");

        	    } catch (IOException | TimeoutException e) {

        	        System.err.println(
        	                "Failed to republish retry message.");

        	        e.printStackTrace();

        	    } finally {

        	        if (publishChannel != null
        	                && publishChannel.isOpen()) {

        	            try {

        	                publishChannel.close();

        	            } catch (Exception e) {

        	                System.err.println(
        	                        "Failed to close retry publish channel.");

        	                e.printStackTrace();
        	            }
        	        }
        	    }

        	} else {

        	    System.err.println(
        	            "Maximum retry limit reached. Sending message to DLQ. DeliveryTag="
        	                    + deliveryTag);

        	    this.getChannel().basicNack(
        	            deliveryTag,
        	            false,
        	            false);

        	    System.out.println(
        	            "basicNack sent successfully. Message routed to DLQ.");
        	}
        }

    } catch (IOException e) {

        System.err.println(
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

        System.err.println(
                "Consumer shutdown unexpectedly: "
                        + sig.getMessage());

    } else {

        System.out.println(
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

        System.out.println(
                "Initializing Consumer for queue: "
                        + this.queueName);
    }

    private void setupConnectionAndChannel()
            throws IOException, TimeoutException {

        ConnectionManager.initialize();

        this.connection =
                ConnectionManager.getConnection();

        System.out.println(
                "Consumer obtained connection: "
                        + this.connection);

        this.channel =
                this.connection.createChannel();

        System.out.println(
                "Consumer created channel: "
                        + this.channel);

        this.channel.addShutdownListener(reason -> {

            if (!reason.isInitiatedByApplication()) {

                System.err.println(
                        "Consumer channel closed unexpectedly: "
                                + reason.getMessage());

            } else {

                System.out.println(
                        "Consumer channel closed by application.");
            }
        });
    }

    public void initializeAndStart()
            throws IOException, TimeoutException {

        System.out.println(
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

        System.out.println(
                "Dead-letter queue declared: "
                        + deadLetterQueue.getQueue());

        channel.queueBind(
                DLQ_NAME,
                DLX_NAME,
                "#"
        );

        System.out.println(
                "Dead-letter queue bound successfully. DLQ="
                        + DLQ_NAME
                        + ", DLX="
                        + DLX_NAME);

        System.out.println(
                "Queue bound successfully. Queue="
                        + this.queueName
                        + ", RoutingKey="
                        + routingKey);

        System.out.println(
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

        System.out.println(
                "Starting consumer on queue: "
                        + this.queueName);

        DefaultConsumer messageHandler =
                new TaskMessageHandler(
                        this.channel);

        try {
        	channel.basicQos(1);
        	System.out.println("Prefetch count set to 1.");
            channel.basicConsume(
                    this.queueName,
                    false,
                    "",
                    messageHandler
            );

            System.out.println(
                    "basicConsume registered successfully for queue: "
                            + this.queueName);

        } catch (IOException e) {

            System.err.println(
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

            System.err.println(
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

            System.err.println(
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