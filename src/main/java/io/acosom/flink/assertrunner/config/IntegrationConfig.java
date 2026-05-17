package io.acosom.flink.assertrunner.config;

import io.acosom.flink.assertrunner.error.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class IntegrationConfig {

    private final String testDataDir;
    private final String kafkaBootstrap;
    private final String schemaRegistryUrl;
    private final String flinkJobmanagerUrl;
    private final List<String> flinkJobProgramArgs;
    private final String flinkJobEntrypointClass;
    private final List<String> outputTopics;
    private final Long successTimeoutMs;

    private IntegrationConfig(Builder b) {
        this.testDataDir = require("testDataDir", b.testDataDir);
        this.kafkaBootstrap = require("kafkaBootstrap", b.kafkaBootstrap);
        this.schemaRegistryUrl = require("schemaRegistryUrl", b.schemaRegistryUrl);
        this.flinkJobmanagerUrl = require("flinkJobmanagerUrl", b.flinkJobmanagerUrl);
        this.flinkJobProgramArgs = requireList("flinkJobProgramArgs", b.flinkJobProgramArgs);
        this.flinkJobEntrypointClass = require("flinkJobEntrypointClass", b.flinkJobEntrypointClass);
        this.outputTopics = b.outputTopics == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.outputTopics);
        this.successTimeoutMs = b.successTimeoutMs;
    }

    public String getTestDataDir() {
        return testDataDir;
    }

    public String getKafkaBootstrap() {
        return kafkaBootstrap;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getFlinkJobmanagerUrl() {
        return flinkJobmanagerUrl;
    }

    /**
     * Program arguments passed verbatim to the Flink job JAR's {@code main()}.
     * Configured via the {@code INTEGRATION_FLINK_JOB_PROGRAM_ARGS} env var
     * as a single whitespace-separated string (e.g.
     * {@code "--sqlfile /opt/flink/sql/x.sql"} or {@code "/path/to/x.sql"}
     * or {@code "-f x.sql --mode batch"}). The assert runner makes no
     * assumption about the JAR's CLI convention.
     */
    public List<String> getFlinkJobProgramArgs() {
        return flinkJobProgramArgs;
    }

    public String getFlinkJobEntrypointClass() {
        return flinkJobEntrypointClass;
    }

    public List<String> getOutputTopics() {
        return outputTopics;
    }

    public Long getSuccessTimeoutMs() {
        return successTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String require(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new ConfigException(name + " is required");
        }
        return value;
    }

    private static List<String> requireList(String name, List<String> value) {
        if (value == null || value.isEmpty()) {
            throw new ConfigException(name + " is required");
        }
        return Collections.unmodifiableList(value);
    }

    public static final class Builder {
        private String testDataDir;
        private String kafkaBootstrap;
        private String schemaRegistryUrl;
        private String flinkJobmanagerUrl;
        private List<String> flinkJobProgramArgs;
        private String flinkJobEntrypointClass;
        private List<String> outputTopics;
        private Long successTimeoutMs;

        public Builder testDataDir(String v) { this.testDataDir = v; return this; }
        public Builder kafkaBootstrap(String v) { this.kafkaBootstrap = v; return this; }
        public Builder schemaRegistryUrl(String v) { this.schemaRegistryUrl = v; return this; }
        public Builder flinkJobmanagerUrl(String v) { this.flinkJobmanagerUrl = v; return this; }
        public Builder flinkJobEntrypointClass(String v) { this.flinkJobEntrypointClass = v; return this; }
        public Builder successTimeoutMs(Long v) { this.successTimeoutMs = v; return this; }

        /**
         * Accepts a single whitespace-separated string (matching how env vars
         * naturally express a list) and splits it into individual tokens. The
         * tokens are passed verbatim to the Flink job JAR's main().
         */
        public Builder flinkJobProgramArgs(String v) {
            if (v == null || v.isBlank()) {
                this.flinkJobProgramArgs = Collections.emptyList();
            } else {
                this.flinkJobProgramArgs = Arrays.asList(v.trim().split("\\s+"));
            }
            return this;
        }

        public Builder outputTopicsCsv(String csv) {
            if (csv == null || csv.isBlank()) {
                this.outputTopics = Collections.emptyList();
            } else {
                this.outputTopics = Arrays.asList(csv.split(","));
            }
            return this;
        }

        public IntegrationConfig build() {
            return new IntegrationConfig(this);
        }
    }
}
