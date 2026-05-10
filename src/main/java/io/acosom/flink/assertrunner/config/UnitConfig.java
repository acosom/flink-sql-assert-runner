package io.acosom.flink.assertrunner.config;

import io.acosom.flink.assertrunner.error.ConfigException;

public final class UnitConfig {

    private final String javaTestDir;
    private final String sqlScriptDir;
    private final String inputEventsDir;

    public UnitConfig(String javaTestDir, String sqlScriptDir, String inputEventsDir) {
        if (javaTestDir == null) {
            throw new ConfigException("UNIT_TEST_JAVA_DIR is required");
        }
        this.javaTestDir = javaTestDir;
        this.sqlScriptDir = sqlScriptDir;
        this.inputEventsDir = inputEventsDir;
    }

    public String getJavaTestDir() {
        return javaTestDir;
    }

    public String getSqlScriptDir() {
        return sqlScriptDir;
    }

    public String getInputEventsDir() {
        return inputEventsDir;
    }
}
