CREATE TABLE OUTPUT_SOURCE
(
    id      STRING NOT NULL,
    uuid    STRING,
    name STRING NOT NULL
) WITH (
      'connector' = 'kafka',
      'topic' = 'it_output_topic',
      'properties.bootstrap.servers' = '@@PROPERTIES_BOOTSTRAP_SERVERS@@',
      'properties.group.id' = 'output-topic-source-id',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = '@@VALUE_AVRO_CONFLUENT_URL@@'
      );

SELECT * FROM OUTPUT_SOURCE WHERE id NOT IN ('1', '2', '3');