package io.acosom.flink.assertrunner.integration;

import io.acosom.flink.assertrunner.config.IntegrationConfig;
import io.acosom.flink.assertrunner.flink.JobController;
import io.acosom.flink.assertrunner.flink.SqlAssertionExecutor;
import io.acosom.flink.assertrunner.kafka.KafkaFixtureLoader;
import io.acosom.flink.assertrunner.kafka.KafkaOutputVerifier;
import io.acosom.flink.assertrunner.kafka.TopicAdmin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Drives a per-folder integration test cycle:
 * <ul>
 *   <li>publishes Avro fixtures from {@code input/} into Kafka</li>
 *   <li>optionally validates Kafka {@code output/} fixtures end-to-end</li>
 *   <li>optionally runs SQL assertion scripts from {@code sqlAssertions/}</li>
 * </ul>
 *
 * The {@link IntegrationConfig} is picked up from a thread-local set by
 * {@link IntegrationContext} so this class can be instantiated by JUnit's
 * default no-arg constructor.
 */
public final class IntegrationSuite {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrationSuite.class);

    private static final String INPUT_FOLDER = "input";
    private static final String OUTPUT_FOLDER = "output";
    private static final String SQL_ASSERTIONS_FOLDER = "sqlAssertions";
    private static final long OUTPUT_VALIDATE_TIMEOUT_MS = 10_000L;

    private IntegrationConfig config;
    private JobController jobController;
    private SqlAssertionExecutor assertionExecutor;
    private KafkaFixtureLoader fixtureLoader;
    private KafkaOutputVerifier outputVerifier;
    private TopicAdmin topicAdmin;

    @Before
    public void setUp() {
        this.config = IntegrationContext.require();
        this.jobController = new JobController(
                config.getFlinkJobmanagerUrl(),
                config.getFlinkJobEntrypointClass(),
                config.getFlinkJobSqlFile());
        this.assertionExecutor = new SqlAssertionExecutor(
                config.getFlinkJobmanagerUrl(),
                "target/flink-sql-assert-runner.jar",
                config.getSuccessTimeoutMs());
        this.fixtureLoader = new KafkaFixtureLoader(
                config.getKafkaBootstrap(), config.getSchemaRegistryUrl());
        this.outputVerifier = new KafkaOutputVerifier(
                config.getKafkaBootstrap(), config.getSchemaRegistryUrl());
        this.topicAdmin = new TopicAdmin(config.getKafkaBootstrap());
    }

    @Test
    public void runAllScenarios() throws Exception {
        File root = new File(config.getTestDataDir());
        if (!root.isDirectory()) {
            throw new IllegalStateException("Test data dir is not a directory: " + root);
        }
        File[] folders = root.listFiles(File::isDirectory);
        if (folders == null) {
            return;
        }
        for (File folder : folders) {
            jobController.cancelAllRunningJobs();
            runScenario(folder);
            jobController.cancelAllRunningJobs();
        }
        jobController.close();
    }

    private void runScenario(File scenario) {
        LOG.info("Running scenario {}", scenario.getName());
        File inputDir = new File(scenario, INPUT_FOLDER);
        File outputDir = new File(scenario, OUTPUT_FOLDER);
        File sqlAssertionsDir = new File(scenario, SQL_ASSERTIONS_FOLDER);

        if (!inputDir.isDirectory()) {
            throw new IllegalStateException("Missing input/ folder in " + scenario);
        }

        purgeConfiguredOutputTopics();
        publishFixtures(inputDir);

        if (outputDir.isDirectory()) {
            validateKafkaOutput(outputDir);
        }
        if (sqlAssertionsDir.isDirectory()) {
            runSqlAssertions(sqlAssertionsDir);
        }
    }

    private void purgeConfiguredOutputTopics() {
        config.getOutputTopics().forEach(topicAdmin::purge);
    }

    private void publishFixtures(File inputDir) {
        File[] inputs = inputDir.listFiles((d, name) -> name.endsWith(".json"));
        if (inputs == null) {
            return;
        }
        for (File inputFile : inputs) {
            if (!inputFile.isFile()) {
                continue;
            }
            String topic = stripExtension(inputFile.getName());
            topicAdmin.purge(topic);
            File schemaFile = new File(inputDir, "schema/" + inputFile.getName());
            fixtureLoader.publish(inputFile, schemaFile, topic);
        }
    }

    private void validateKafkaOutput(File outputDir) {
        File[] outputs = outputDir.listFiles((d, name) -> name.endsWith(".json"));
        if (outputs == null) {
            return;
        }
        for (File outputFile : outputs) {
            topicAdmin.purge(stripExtension(outputFile.getName()));
        }

        jobController.startJob();
        for (File outputFile : outputs) {
            String topic = stripExtension(outputFile.getName());
            boolean valid = outputVerifier.containsAnyExpected(outputFile, topic, OUTPUT_VALIDATE_TIMEOUT_MS);
            Assert.assertTrue(
                    "Output topic " + topic + " did not contain expected message from " + outputFile.getName(),
                    valid);
        }
    }

    private void runSqlAssertions(File sqlDir) {
        jobController.startJob();
        File[] sqlFiles = sqlDir.listFiles((d, name) -> name.endsWith(".sql"));
        if (sqlFiles == null) {
            return;
        }
        for (File sqlFile : sqlFiles) {
            assertionExecutor.run(sqlFile);
        }
    }

    private static String stripExtension(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot >= 0 ? filename.substring(0, dot) : filename;
    }
}
