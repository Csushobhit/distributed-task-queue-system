package com.sushobhit.taskqueue.common;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    MetricsManager.class);

    private static final CompositeMeterRegistry
            COMPOSITE_REGISTRY =
            new CompositeMeterRegistry();

    private static final PrometheusMeterRegistry
            PROMETHEUS_REGISTRY;

    static {

        PROMETHEUS_REGISTRY =
                new PrometheusMeterRegistry(
                        PrometheusConfig.DEFAULT);

        COMPOSITE_REGISTRY.add(
                PROMETHEUS_REGISTRY);

        LOGGER.info(
                "CompositeMeterRegistry initialized with PrometheusMeterRegistry.");
    }

    public static MeterRegistry getRegistry() {

        return COMPOSITE_REGISTRY;
    }

    public static PrometheusMeterRegistry
    getPrometheusRegistry() {

        return PROMETHEUS_REGISTRY;
    }

    private MetricsManager() {
    }
}