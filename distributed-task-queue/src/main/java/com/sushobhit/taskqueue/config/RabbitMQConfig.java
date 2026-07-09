package com.sushobhit.taskqueue.config;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQConfig {

    private static final String HOST = "localhost";
    private static final int PORT = 5672;

    private static final String USERNAME = "task_user";
    private static final String PASSWORD = "DEep@5515";

    private static final String VIRTUAL_HOST = "task_queue_vhost";

    public static ConnectionFactory createConnectionFactory() {

        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(HOST);
        factory.setPort(PORT);

        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);

        factory.setVirtualHost(VIRTUAL_HOST);

        return factory;
    }
}