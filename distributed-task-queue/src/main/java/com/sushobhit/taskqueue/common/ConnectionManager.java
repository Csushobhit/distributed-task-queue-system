package com.sushobhit.taskqueue.common;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sushobhit.taskqueue.config.RabbitMQConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager implements AutoCloseable {
	
	private static final Logger LOGGER =
	        LoggerFactory.getLogger(ConnectionManager.class);

    private static final AtomicReference<Connection> connection =
            new AtomicReference<>(null);

    private static ConnectionFactory factory;

    public static void initialize() {

        if (factory == null) {

            factory = RabbitMQConfig.createConnectionFactory();

            factory.setAutomaticRecoveryEnabled(true);

            LOGGER.info(
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

                	LOGGER.info(
                            "Creating new RabbitMQ connection...");

                    try {

                        currentConnection = factory.newConnection();

                        connection.set(currentConnection);

                        LOGGER.info(
                                "RabbitMQ connection established.");

                        final Connection listenerConnection = currentConnection;

                        currentConnection.addShutdownListener(reason -> {

                            if (!reason.isInitiatedByApplication()) {

                            	LOGGER.error(
                                        "RabbitMQ connection closed unexpectedly: "
                                                + reason.getMessage());

                                connection.compareAndSet(
                                        listenerConnection,
                                        null);

                            } else {

                            	LOGGER.info(
                                        "RabbitMQ connection closed by application.");
                            }
                        });

                    } catch (IOException | TimeoutException e) {

                        connection.set(null);

                        LOGGER.error(
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

            	LOGGER.info(
                        "Closing RabbitMQ connection...");

                currentConnection.close();

            } catch (IOException e) {

            	LOGGER.error(
                        "Error while closing RabbitMQ connection: "
                                + e.getMessage());

            } finally {

                connection.set(null);
            }

        } else {

        	LOGGER.info(
                    "RabbitMQ connection already closed.");
        }
    }

    private ConnectionManager() {
    }
}