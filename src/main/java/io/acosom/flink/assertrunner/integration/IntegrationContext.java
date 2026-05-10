package io.acosom.flink.assertrunner.integration;

import io.acosom.flink.assertrunner.config.IntegrationConfig;

/**
 * Bridges the JUnit-instantiated {@link IntegrationSuite} (no-arg constructor)
 * to the {@link IntegrationConfig} built by the CLI: the CLI sets the config
 * before invoking JUnit, and the suite reads it back during {@code @Before}.
 */
public final class IntegrationContext {

    private static volatile IntegrationConfig CONFIG;

    private IntegrationContext() {
    }

    public static void set(IntegrationConfig config) {
        CONFIG = config;
    }

    public static IntegrationConfig require() {
        IntegrationConfig cfg = CONFIG;
        if (cfg == null) {
            throw new IllegalStateException(
                    "IntegrationConfig was not set. Call IntegrationContext.set(...) before running the suite.");
        }
        return cfg;
    }

    public static void clear() {
        CONFIG = null;
    }
}
