CREATE TABLE INPUT_TOPIC_SOURCE_1
(
    id STRING,
    name STRING,
    description STRING
) WITH (
      'connector' = 'kafka',
      'properties.bootstrap.servers' = 'kafka_broker:29092',
      'properties.group.id' = 'input-topic-group-id',
      'properties.security.protocol' = 'PLAINTEXT',
      'properties.auto.offset.reset' = 'earliest',
      'topic' = 'input_topic',
      'value.format' = 'json'
      );


CREATE VIEW VIEW_EXAMPLE_1 AS
SELECT
    id as id,
    name as name,
    description as details
FROM INPUT_TOPIC_SOURCE_1;


CREATE TABLE OUTPUT_TOPIC_SINK_1
(
    id STRING,
    name STRING,
    details STRING
)
    WITH (
      'connector' = 'filesystem',
      'format' = 'csv',
      'path' = '/tmp/folder',
      'sink.rolling-policy.check-interval' = '5 s ',
      'sink.rolling-policy.rollover-interval' = '1 m',
      'sink.partition-commit.trigger' = 'process-time',
      'sink.partition-commit.delay' = '1 m',
      'sink.partition-commit.policy.kind' = 'success-file'
      );


CREATE TABLE INPUT_TOPIC_SOURCE_2
(
    id                STRING,
    inputId           STRING,
    name       STRING
) WITH (
      'connector' = 'kafka',
      'properties.bootstrap.servers' = 'kafka_broker:29092',
      'properties.group.id' = 'input-topic-2-group',
      'properties.security.protocol' = 'PLAINTEXT',
      'properties.auto.offset.reset' = 'earliest',
      'topic' = 'input_topic_2',
      'value.format' = 'json'
      );


CREATE VIEW VIEW_EXAMPLE_2 AS
SELECT
    b.id as id,
    b.inputId as inputId,
    b.name as name
FROM INPUT_TOPIC_SOURCE_2 b
         INNER JOIN INPUT_TOPIC_SOURCE_1 as a
                    ON b.inputId = a.id;


CREATE TABLE OUTPUT_TOPIC_SINK_2
(
    id STRING,
    inputId STRING,
    name STRING
)
WITH (
  'connector' = 'filesystem',
  'format' = 'csv',
  'path' = '/tmp/folder',
  'sink.rolling-policy.check-interval' = '5 s ',
  'sink.rolling-policy.rollover-interval' = '1 m',
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay' = '1 m',
  'sink.partition-commit.policy.kind' = 'success-file'
    );


EXECUTE STATEMENT SET
BEGIN
INSERT INTO OUTPUT_TOPIC_SINK_1
SELECT
    id,
    name,
    details
FROM VIEW_EXAMPLE_1;
INSERT INTO OUTPUT_TOPIC_SINK_2
SELECT      id,
            inputId,
            name
FROM VIEW_EXAMPLE_2;
END;