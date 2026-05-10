#!/bin/sh
set -e

echo "Starting Flink SQL Assert Runner"

require() {
  name=$1
  eval "value=\$$name"
  if [ -z "$value" ]; then
    echo "ERROR: required env var $name is not set"
    exit 1
  fi
}

require RESULT_FILE

if [ "$RUN_UNIT_TESTS" = "true" ]; then
  require UNIT_TEST_JAVA_DIR
else
  require INTEGRATION_KAFKA_SERVER
  require INTEGRATION_SCHEMA_REGISTRY_URL
  require INTEGRATION_FLINK_JOBMANAGER_SERVER
  require INTEGRATION_TEST_DATA_DIR
  require INTEGRATION_TEST_JOB_SQL_FILE
  require INTEGRATION_FLINK_JOB_ENTRYPOINT_CLASS
fi

java --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.util=ALL-UNNAMED \
     -jar /app/flink-sql-assert-runner.jar
java_exit_code=$?

if [ "$java_exit_code" -ne 0 ]; then
  exit 1
fi

if grep -q "Result: Fail" "$RESULT_FILE"; then
  exit 1
fi
