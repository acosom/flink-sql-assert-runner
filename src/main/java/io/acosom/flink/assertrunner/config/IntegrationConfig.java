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
    private final String flinkJobSqlFile;
    private final String flinkJobEntrypointClass;
    private final List<String> outputTopics;
    private final Long successTimeoutMs;

    private IntegrationConfig(Builder b) {
        this.testDataDir = require("testDataDir", b.testDataDir);
        this.kafkaBootstrap = require("kafkaBootstrap", b.kafkaBootstrap);
        this.schemaRegistryUrl = require("schemaRegistryUrl", b.schemaRegistryUrl);
        this.flinkJobmanagerUrl = require("flinkJobmanagerUrl", b.flinkJobmanagerUrl);
        this.flinkJobSqlFile = require("flinkJobSqlFile", b.flinkJobSqlFile);
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

    public String getFlinkJobSqlFile() {
        return flinkJobSqlFile;
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

    public static final class Builder {
        private String testDataDir;
        private String kafkaBootstrap;
        private String schemaRegistryUrl;
        private String flinkJobmanagerUrl;
        private String flinkJobSqlFile;
        private String flinkJobEntrypointClass;
        private List<String> outputTopics;
        private Long successTimeoutMs;

        public Builder testDataDir(String v) { this.testDataDir = v; return this; }
        public Builder kafkaBootstrap(String v) { this.kafkaBootstrap = v; return this; }
        public Builder schemaRegistryUrl(String v) { this.schemaRegistryUrl = v; return this; }
        public Builder flinkJobmanagerUrl(String v) { this.flinkJobmanagerUrl = v; return this; }
        public Builder flinkJobSqlFile(String v) { this.flinkJobSqlFile = v; return this; }
        public Builder flinkJobEntrypointClass(String v) { this.flinkJobEntrypointClass = v; return this; }
        public Builder successTimeoutMs(Long v) { this.successTimeoutMs = v; return this; }

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
