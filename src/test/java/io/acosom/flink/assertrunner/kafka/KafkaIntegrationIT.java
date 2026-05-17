package io.acosom.flink.assertrunner.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class KafkaIntegrationIT {

    private static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.2.4"));

    private static String bootstrapServers;
    private static String schemaRegistryUrl;

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"asset\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"uuid\",\"type\":\"string\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"}]}";

    private static final String INPUT_JSON =
            "[{\"id\":\"1\",\"uuid\":\"abc1\",\"name\":\"TEST 1\"},"
                    + "{\"id\":\"2\",\"uuid\":\"abc2\",\"name\":\"TEST 2\"}]";

    @BeforeAll
    static void startContainers() {
        REDPANDA.start();
        bootstrapServers = REDPANDA.getBootstrapServers();
        schemaRegistryUrl = REDPANDA.getSchemaRegistryAddress();
    }

    @AfterAll
    static void stopContainers() {
        REDPANDA.stop();
    }

    @Test
    void fixtureLoaderPublishesAvroRecordsThatAreConsumable(@TempDir Path tempDir) throws Exception {
        String topic = "fixture-load-" + UUID.randomUUID();
        File input = writeFile(tempDir, "input.json", INPUT_JSON);
        File schema = writeFile(tempDir, "schema.json", SCHEMA_JSON);

        new KafkaFixtureLoader(bootstrapServers, schemaRegistryUrl).publish(input, schema, topic);

        List<JsonNode> consumed = consumeAll(topic, 2, Duration.ofSeconds(15));
        assertThat(consumed).hasSize(2);
        assertThat(consumed.get(0).get("name").asText()).isEqualTo("TEST 1");
        assertThat(consumed.get(1).get("name").asText()).isEqualTo("TEST 2");
    }

    @Test
    void verifierReturnsTrueWhenExpectedMessagePresent(@TempDir Path tempDir) throws Exception {
        String topic = "verify-true-" + UUID.randomUUID();
        File input = writeFile(tempDir, "input.json", INPUT_JSON);
        File schema = writeFile(tempDir, "schema.json", SCHEMA_JSON);
        File expected = writeFile(tempDir, "expected.json",
                "[{\"id\":\"2\",\"uuid\":\"abc2\",\"name\":\"TEST 2\"}]");

        new KafkaFixtureLoader(bootstrapServers, schemaRegistryUrl).publish(input, schema, topic);

        boolean valid = new KafkaOutputVerifier(bootstrapServers, schemaRegistryUrl)
                .containsAnyExpected(expected, topic, 15_000L);

        assertThat(valid).isTrue();
    }

    @Test
    void verifierReturnsFalseWhenNoMatchWithinTimeout(@TempDir Path tempDir) throws Exception {
        String topic = "verify-false-" + UUID.randomUUID();
        File input = writeFile(tempDir, "input.json", INPUT_JSON);
        File schema = writeFile(tempDir, "schema.json", SCHEMA_JSON);
        File expected = writeFile(tempDir, "expected.json",
                "[{\"id\":\"99\",\"uuid\":\"nope\",\"name\":\"NOPE\"}]");

        new KafkaFixtureLoader(bootstrapServers, schemaRegistryUrl).publish(input, schema, topic);

        boolean valid = new KafkaOutputVerifier(bootstrapServers, schemaRegistryUrl)
                .containsAnyExpected(expected, topic, 2_000L);

        assertThat(valid).isFalse();
    }

    @Test
    void topicAdminPurgeClearsExistingMessages(@TempDir Path tempDir) throws Exception {
        String topic = "purge-test-" + UUID.randomUUID();
        File input = writeFile(tempDir, "input.json", INPUT_JSON);
        File schema = writeFile(tempDir, "schema.json", SCHEMA_JSON);

        KafkaFixtureLoader loader = new KafkaFixtureLoader(bootstrapServers, schemaRegistryUrl);
        loader.publish(input, schema, topic);
        assertThat(consumeAll(topic, 1, Duration.ofSeconds(10))).isNotEmpty();

        new TopicAdmin(bootstrapServers).purge(topic);

        assertThat(consumeAll(topic, 1, Duration.ofSeconds(3))).isEmpty();
    }

    private static List<JsonNode> consumeAll(String topic, int minMessages, Duration timeout) throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (System.currentTimeMillis() < deadline && collected.size() < minMessages) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    collected.add(mapper.readTree(record.value().toString()));
                }
            }
        }
        return collected;
    }

    private static File writeFile(Path dir, String name, String content) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, content);
        return file.toFile();
    }
}
