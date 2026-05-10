CREATE TABLE OUTPUT_SOURCE
(
    id      STRING NOT NULL,
    uuid    STRING,
    name STRING NOT NULL
) WITH (
      'connector' = 'kafka',
      'topic' = 'output_topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'properties.security.protocol' = '@@PROPERTIES_SECURITY_PROTOCOL@@',
      'properties.sasl.mechanism' = '@@PROPERTIES_SECURITY_MECHANISM@@',
      'properties.sasl.jaas.config' = '@@PROPERTIES_SECURITY_JAAS_CONFIG@@',
      'properties.group.id' = 'output-topic-source-group-id-1',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@'
      );

SELECT * FROM OUTPUT_SOURCE; -- outputCount:2 mode:positive
