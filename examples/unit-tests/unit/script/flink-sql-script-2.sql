CREATE TABLE INPUT_SOURCE
(
    id      STRING NOT NULL,
    uuid    STRING,
    name STRING NOT NULL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'input-topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'properties.group.id' = 'input-group-id',
      'properties.security.protocol' = '@@PROPERTIES_SECURITY_PROTOCOL@@',
      'properties.sasl.mechanism' = '@@PROPERTIES_SECURITY_MECHANISM@@',
      'properties.sasl.jaas.config' = '@@PROPERTIES_SECURITY_JAAS_CONFIG@@',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@',
      'key.format' = 'raw'
      );

CREATE TABLE OUTPUT_SINK
(
    id      STRING NOT NULL,
    name    STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'output-topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'properties.group.id' = 'output-group-id',
      'properties.security.protocol' = '@@PROPERTIES_SECURITY_PROTOCOL@@',
      'properties.sasl.mechanism' = '@@PROPERTIES_SECURITY_MECHANISM@@',
      'properties.sasl.jaas.config' = '@@PROPERTIES_SECURITY_JAAS_CONFIG@@',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@',
      'key.format' = 'raw',
      'auto-create.enable' = 'true',
      'auto-create.partitions' = '@@TOPIC_AUTO_CREATE_PARTITIONS@@',
      'auto-create.replication-factor' = '@@TOPIC_AUTO_CREATE_REPLICATION_FACTOR@@',
      'auto-create.config.cleanup.policy' = 'compact'
      );

EXECUTE STATEMENT SET
BEGIN

INSERT INTO OUTPUT_SINK
SELECT id
     , name
FROM INPUT_SOURCE
WHERE id IN ('1', '2');

END;
