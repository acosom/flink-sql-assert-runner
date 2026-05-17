-- Reference pipeline used by the integration CI flow.
-- Reads from it_input_topic, filters on id < 3, writes to it_output_topic.
-- @@PLACEHOLDERS@@ resolved on the JobManager by acosom/flink-sql-runner
-- using the env vars set in ci/integration.env.

CREATE TABLE INPUT_SOURCE
(
    id      STRING NOT NULL,
    uuid    STRING,
    name    STRING NOT NULL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'it_input_topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'properties.group.id' = 'input-group-id',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@',
      'key.format' = 'raw'
);

CREATE TABLE OUTPUT_SINK
(
    id      STRING NOT NULL,
    uuid    STRING,
    name    STRING NOT NULL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'it_output_topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@',
      'key.format' = 'raw',
      'auto-create.enable' = 'true',
      'auto-create.partitions' = '@@TOPIC_AUTO_CREATE_PARTITIONS@@',
      'auto-create.replication-factor' = '@@TOPIC_AUTO_CREATE_REPLICATION_FACTOR@@'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO OUTPUT_SINK
SELECT id, uuid, name
FROM INPUT_SOURCE
WHERE id < '3';
END;
