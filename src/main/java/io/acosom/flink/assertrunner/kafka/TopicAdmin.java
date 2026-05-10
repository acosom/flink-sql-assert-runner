package io.acosom.flink.assertrunner.kafka;

import io.acosom.flink.assertrunner.error.KafkaSetupException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle operations against Kafka topics: delete + recreate ("purge") so
 * tests start from a known clean state.
 */
public final class TopicAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(TopicAdmin.class);

    private static final int MAX_DELETE_RETRIES = 5;
    private static final long DELETE_RETRY_DELAY_MS = 1_000L;
    private static final long ADMIN_OP_TIMEOUT_SECONDS = 10L;

    private final String bootstrapServers;

    public TopicAdmin(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Deletes the topic if it exists, waits for deletion, then recreates it
     * with a single partition.
     */
    public void purge(String topic) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-sql-assert-runner-" + UUID.randomUUID());

        try (AdminClient admin = AdminClient.create(props)) {
            try {
                admin.deleteTopics(List.of(topic)).all().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException notFound) {
                LOG.debug("Topic {} did not exist, will create fresh", topic);
            }

            waitForDeletion(admin, topic);
            createWithRetries(admin, topic);
            LOG.info("Topic {} purged and recreated", topic);
        } catch (KafkaSetupException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaSetupException("Failed to purge topic " + topic, e);
        }
    }

    private static void createWithRetries(AdminClient admin, String topic) throws InterruptedException {
        for (int attempt = 0; attempt < MAX_DELETE_RETRIES; attempt++) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    Thread.sleep(DELETE_RETRY_DELAY_MS);
                    continue;
                }
                throw new KafkaSetupException("Failed to create topic " + topic, e);
            } catch (Exception e) {
                throw new KafkaSetupException("Failed to create topic " + topic, e);
            }
        }
        throw new KafkaSetupException("Topic " + topic + " could not be recreated after retries");
    }

    private static void waitForDeletion(AdminClient admin, String topic) throws InterruptedException {
        for (int i = 0; i < MAX_DELETE_RETRIES; i++) {
            try {
                admin.describeTopics(List.of(topic))
                        .values()
                        .get(topic)
                        .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception gone) {
                return;
            }
            Thread.sleep(DELETE_RETRY_DELAY_MS);
        }
        throw new KafkaSetupException("Topic " + topic + " was not deleted within timeout");
    }
}
