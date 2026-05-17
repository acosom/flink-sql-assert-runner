package io.acosom.flink.assertrunner.config;

import java.util.Map;

public final class ConfigLoader {

    public static final String UNIT_TEST_JAVA_DIR_DEFAULT = "/app/test";
    public static final String UNIT_TEST_SQL_DIR_DEFAULT = "/opt/flink/sql";
    public static final String UNIT_TEST_INPUT_EVENTS_DIR_DEFAULT = "/app/input";

    private ConfigLoader() {
    }

    public static RunnerConfig fromEnv(Map<String, String> env) {
        boolean unitMode = Boolean.parseBoolean(env.getOrDefault("RUN_UNIT_TESTS", "false"));
        String resultFile = env.get("RESULT_FILE");

        if (unitMode) {
            UnitConfig unit = new UnitConfig(
                    env.getOrDefault("UNIT_TEST_JAVA_DIR", UNIT_TEST_JAVA_DIR_DEFAULT),
                    env.getOrDefault("UNIT_TEST_SQL_DIR", UNIT_TEST_SQL_DIR_DEFAULT),
                    env.getOrDefault("UNIT_TEST_INPUT_EVENTS_DIR", UNIT_TEST_INPUT_EVENTS_DIR_DEFAULT));
            return RunnerConfig.forUnit(resultFile, unit);
        }

        IntegrationConfig integration = IntegrationConfig.builder()
                .testDataDir(env.get("INTEGRATION_TEST_DATA_DIR"))
                .kafkaBootstrap(env.get("INTEGRATION_KAFKA_SERVER"))
                .schemaRegistryUrl(env.get("INTEGRATION_SCHEMA_REGISTRY_URL"))
                .flinkJobmanagerUrl(env.get("INTEGRATION_FLINK_JOBMANAGER_SERVER"))
                .flinkJobProgramArgs(env.get("INTEGRATION_FLINK_JOB_PROGRAM_ARGS"))
                .flinkJobEntrypointClass(env.get("INTEGRATION_FLINK_JOB_ENTRYPOINT_CLASS"))
                .assertRunnerJarPath(env.get("INTEGRATION_ASSERT_RUNNER_JAR_PATH"))
                .outputTopicsCsv(env.get("INTEGRATION_OUTPUT_TOPICS"))
                .successTimeoutMs(parseLong(env.get("INTEGRATION_TEST_SUCCESS_TIMEOUT_MS")))
                .build();
        return RunnerConfig.forIntegration(resultFile, integration);
    }

    private static Long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
