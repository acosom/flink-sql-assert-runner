package io.acosom.flink.assertrunner.config;

import io.acosom.flink.assertrunner.error.ConfigException;

public final class RunnerConfig {

    private final boolean unitMode;
    private final String resultFile;
    private final UnitConfig unit;
    private final IntegrationConfig integration;

    private RunnerConfig(boolean unitMode, String resultFile,
                         UnitConfig unit, IntegrationConfig integration) {
        this.unitMode = unitMode;
        this.resultFile = resultFile;
        this.unit = unit;
        this.integration = integration;
    }

    public static RunnerConfig forUnit(String resultFile, UnitConfig unit) {
        if (resultFile == null) {
            throw new ConfigException("resultFile is required");
        }
        if (unit == null) {
            throw new ConfigException("unit config is required for unit mode");
        }
        return new RunnerConfig(true, resultFile, unit, null);
    }

    public static RunnerConfig forIntegration(String resultFile, IntegrationConfig integration) {
        if (resultFile == null) {
            throw new ConfigException("resultFile is required");
        }
        if (integration == null) {
            throw new ConfigException("integration config is required for integration mode");
        }
        return new RunnerConfig(false, resultFile, null, integration);
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public String getResultFile() {
        return resultFile;
    }

    public UnitConfig getUnit() {
        if (!unitMode) {
            throw new IllegalStateException("not in unit mode");
        }
        return unit;
    }

    public IntegrationConfig getIntegration() {
        if (unitMode) {
            throw new IllegalStateException("not in integration mode");
        }
        return integration;
    }
}
