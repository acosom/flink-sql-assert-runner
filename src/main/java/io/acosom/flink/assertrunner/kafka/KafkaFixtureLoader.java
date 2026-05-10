package io.acosom.flink.assertrunner.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.acosom.flink.assertrunner.error.KafkaSetupException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Publishes Avro-encoded JSON fixtures to a Kafka topic. Each element of the
 * input JSON array is keyed by its {@code id} field.
 */
public final class KafkaFixtureLoader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFixtureLoader.class);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public KafkaFixtureLoader(String bootstrapServers, String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public void publish(File inputFile, File schemaFile, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(inputFile);
            if (!root.isArray()) {
                LOG.warn("Input file {} is not a JSON array, skipping", inputFile.getName());
                return;
            }

            Schema avroSchema = new Schema.Parser().parse(mapper.readTree(schemaFile).toString());
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);

            for (JsonNode messageNode : root) {
                if (!messageNode.isObject()) {
                    LOG.warn("Skipping non-object JSON element in {}", inputFile.getName());
                    continue;
                }
                String key = messageNode.get("id").asText();
                Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, messageNode.toString());
                GenericRecord record = datumReader.read(null, decoder);
                producer.send(new ProducerRecord<>(topic, key, record), (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Publish failed for topic {}: {}", topic, exception.getMessage());
                    }
                });
            }
        } catch (IOException e) {
            throw new KafkaSetupException(
                    "Failed to publish fixture from " + inputFile + " to topic " + topic, e);
        }
    }
}
