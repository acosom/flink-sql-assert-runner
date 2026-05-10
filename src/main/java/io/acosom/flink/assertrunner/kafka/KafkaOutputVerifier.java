package io.acosom.flink.assertrunner.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.acosom.flink.assertrunner.error.KafkaSetupException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * Consumes from a Kafka topic and checks whether the messages match an
 * expected JSON fixture. Used to validate Flink job output.
 */
public final class KafkaOutputVerifier {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public KafkaOutputVerifier(String bootstrapServers, String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    /**
     * Returns true if the topic eventually produces a message matching at least
     * one of the entries in the expected fixture, within the timeout.
     */
    public boolean containsAnyExpected(File expectedFile, String topic, long timeoutInMs) {
        try (KafkaConsumer<String, GenericRecord> consumer = newConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));

            ObjectMapper mapper = new ObjectMapper();
            JsonNode expected = mapper.readTree(expectedFile);
            if (!expected.isArray()) {
                return false;
            }

            for (JsonNode expectedMessage : expected) {
                long deadline = System.currentTimeMillis() + timeoutInMs;
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(POLL_INTERVAL);
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        JsonNode actual = mapper.readTree(record.value().toString());
                        if (actual.equals(expectedMessage)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        } catch (IOException e) {
            throw new KafkaSetupException(
                    "Failed to validate topic " + topic + " against " + expectedFile, e);
        }
    }

    private KafkaConsumer<String, GenericRecord> newConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-sql-assert-runner-" + UUID.randomUUID());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaConsumer<>(props);
    }
}
