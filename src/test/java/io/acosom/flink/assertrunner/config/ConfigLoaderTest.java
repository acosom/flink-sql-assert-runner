package io.acosom.flink.assertrunner.config;

import io.acosom.flink.assertrunner.error.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    @Test
    void integrationModeRequiresAllIntegrationVars() {
        Map<String, String> env = baseIntegrationEnv();
        env.remove("INTEGRATION_KAFKA_SERVER");

        assertThatThrownBy(() -> ConfigLoader.fromEnv(env))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("kafkaBootstrap");
    }

    @Test
    void integrationModeProducesPopulatedConfig() {
        RunnerConfig cfg = ConfigLoader.fromEnv(baseIntegrationEnv());

        assertThat(cfg.isUnitMode()).isFalse();
        assertThat(cfg.getResultFile()).isEqualTo("/tmp/result.txt");
        IntegrationConfig integration = cfg.getIntegration();
        assertThat(integration.getKafkaBootstrap()).isEqualTo("localhost:9092");
        assertThat(integration.getSchemaRegistryUrl()).isEqualTo("http://localhost:8081");
        assertThat(integration.getFlinkJobmanagerUrl()).isEqualTo("http://localhost:8081");
        assertThat(integration.getFlinkJobEntrypointClass()).isEqualTo("com.example.SqlRunner");
        assertThat(integration.getFlinkJobProgramArgs())
                .containsExactly("--sqlfile", "/opt/flink/sql/job.sql");
        assertThat(integration.getOutputTopics()).containsExactly("a", "b");
        assertThat(integration.getSuccessTimeoutMs()).isEqualTo(5000L);
    }

    @Test
    void unitModeReadsUnitVarsAndAppliesDefaults() {
        Map<String, String> env = new HashMap<>();
        env.put("RUN_UNIT_TESTS", "true");
        env.put("RESULT_FILE", "/tmp/r.txt");

        RunnerConfig cfg = ConfigLoader.fromEnv(env);

        assertThat(cfg.isUnitMode()).isTrue();
        assertThat(cfg.getUnit().getJavaTestDir())
                .isEqualTo(ConfigLoader.UNIT_TEST_JAVA_DIR_DEFAULT);
        assertThat(cfg.getUnit().getSqlScriptDir())
                .isEqualTo(ConfigLoader.UNIT_TEST_SQL_DIR_DEFAULT);
    }

    @Test
    void unitModeRespectsExplicitOverrides() {
        Map<String, String> env = new HashMap<>();
        env.put("RUN_UNIT_TESTS", "true");
        env.put("RESULT_FILE", "/tmp/r.txt");
        env.put("UNIT_TEST_JAVA_DIR", "/custom/java");
        env.put("UNIT_TEST_SQL_DIR", "/custom/sql");
        env.put("UNIT_TEST_INPUT_EVENTS_DIR", "/custom/events");

        RunnerConfig cfg = ConfigLoader.fromEnv(env);

        assertThat(cfg.getUnit().getJavaTestDir()).isEqualTo("/custom/java");
        assertThat(cfg.getUnit().getSqlScriptDir()).isEqualTo("/custom/sql");
        assertThat(cfg.getUnit().getInputEventsDir()).isEqualTo("/custom/events");
    }

    @Test
    void resultFileIsRequired() {
        Map<String, String> env = baseIntegrationEnv();
        env.remove("RESULT_FILE");

        assertThatThrownBy(() -> ConfigLoader.fromEnv(env))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("resultFile");
    }

    @Test
    void emptyOutputTopicsCsvBecomesEmptyList() {
        Map<String, String> env = baseIntegrationEnv();
        env.put("INTEGRATION_OUTPUT_TOPICS", "");

        RunnerConfig cfg = ConfigLoader.fromEnv(env);

        assertThat(cfg.getIntegration().getOutputTopics()).isEmpty();
    }

    @Test
    void invalidSuccessTimeoutBecomesNullRatherThanThrows() {
        Map<String, String> env = baseIntegrationEnv();
        env.put("INTEGRATION_TEST_SUCCESS_TIMEOUT_MS", "not-a-number");

        RunnerConfig cfg = ConfigLoader.fromEnv(env);

        assertThat(cfg.getIntegration().getSuccessTimeoutMs()).isNull();
    }

    private static Map<String, String> baseIntegrationEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("RUN_UNIT_TESTS", "false");
        env.put("RESULT_FILE", "/tmp/result.txt");
        env.put("INTEGRATION_TEST_DATA_DIR", "./data");
        env.put("INTEGRATION_KAFKA_SERVER", "localhost:9092");
        env.put("INTEGRATION_SCHEMA_REGISTRY_URL", "http://localhost:8081");
        env.put("INTEGRATION_FLINK_JOBMANAGER_SERVER", "http://localhost:8081");
        env.put("INTEGRATION_FLINK_JOB_PROGRAM_ARGS", "--sqlfile /opt/flink/sql/job.sql");
        env.put("INTEGRATION_FLINK_JOB_ENTRYPOINT_CLASS", "com.example.SqlRunner");
        env.put("INTEGRATION_OUTPUT_TOPICS", "a,b");
        env.put("INTEGRATION_TEST_SUCCESS_TIMEOUT_MS", "5000");
        return env;
    }
}
