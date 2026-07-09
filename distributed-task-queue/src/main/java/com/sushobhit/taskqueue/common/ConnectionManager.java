package com.sushobhit.taskqueue.common;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sushobhit.taskqueue.config.RabbitMQConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionManager implements AutoCloseable {

    private static final AtomicReference<Connection> connection =
            new AtomicReference<>(null);

    private static ConnectionFactory factory;

    public static void initialize() {

        if (factory == null) {

            factory = RabbitMQConfig.createConnectionFactory();

            factory.setAutomaticRecoveryEnabled(true);

            System.out.println(
                    "ConnectionManager initialized. Automatic recovery enabled.");
        }
    }

    public static Connection getConnection()
            throws IOException, TimeoutException {

        if (factory == null) {

            throw new IllegalStateException(
                    "ConnectionManager has not been initialized. Call initialize() first.");
        }

        Connection currentConnection = connection.get();

        if (currentConnection == null || !currentConnection.isOpen()) {

            synchronized (connection) {

                currentConnection = connection.get();

                if (currentConnection == null
                        || !currentConnection.isOpen()) {

                    System.out.println(
                            "Creating new RabbitMQ connection...");

                    try {

                        currentConnection = factory.newConnection();

                        connection.set(currentConnection);

                        System.out.println(
                                "RabbitMQ connection established.");

                        final Connection listenerConnection = currentConnection;

                        currentConnection.addShutdownListener(reason -> {

                            if (!reason.isInitiatedByApplication()) {

                                System.err.println(
                                        "RabbitMQ connection closed unexpectedly: "
                                                + reason.getMessage());

                                connection.compareAndSet(
                                        listenerConnection,
                                        null);

                            } else {

                                System.out.println(
                                        "RabbitMQ connection closed by application.");
                            }
                        });

                    } catch (IOException | TimeoutException e) {

                        connection.set(null);

                        System.err.println(
                                "Failed to establish RabbitMQ connection.");

                        throw e;
                    }
                }
            }
        }

        return connection.get();
    }

    @Override
    public void close() {

        closeConnection();
    }

    public static synchronized void closeConnection() {

        Connection currentConnection = connection.get();

        if (currentConnection != null
                && currentConnection.isOpen()) {

            try {

                System.out.println(
                        "Closing RabbitMQ connection...");

                currentConnection.close();

            } catch (IOException e) {

                System.err.println(
                        "Error while closing RabbitMQ connection: "
                                + e.getMessage());

            } finally {

                connection.set(null);
            }

        } else {

            System.out.println(
                    "RabbitMQ connection already closed.");
        }
    }

    private ConnectionManager() {
    }
}