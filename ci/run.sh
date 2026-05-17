#!/usr/bin/env bash
#
# Runs the assert-runner end-to-end against a docker-compose stack
# (Kafka + Schema Registry + Flink session cluster + assert-runner image).
# Used both locally and by GitHub Actions.
#
# Usage:
#   ci/run.sh integration   # default: full integration suite
#   ci/run.sh unit          # unit suite only (no Kafka/Flink needed)
#
# Environment overrides:
#   SQL_RUNNER_REPO     Path to a checked-out flink-sql-runner repo (used to
#                       build the JAR that the JobManager runs). Defaults to
#                       ../sql-runner relative to this repo.
#   SKIP_BUILD          Set to "1" to skip mvn jib:dockerBuild + SQL-runner
#                       packaging. Useful when iterating on docker-compose
#                       changes against an already-built image + jar.

set -euo pipefail

PROFILE="${1:-integration}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_RUNNER_REPO="${SQL_RUNNER_REPO:-${REPO_ROOT}/../sql-runner}"
SKIP_BUILD="${SKIP_BUILD:-0}"

case "$PROFILE" in
  integration|unit) ;;
  *)
    echo "Usage: $0 [integration|unit]" >&2
    exit 2
    ;;
esac

cd "$REPO_ROOT"
mkdir -p result flink-jars

# Tells the compose service which env file to load (ci/integration.env vs
# ci/unit.env). The two env files differ on RUN_UNIT_TESTS and which set of
# variables they declare required.
export ENV_FILE="$PROFILE"

if [ "$SKIP_BUILD" = "0" ]; then
  echo "==> Building assert-runner image (mvn -Pdocker package jib:dockerBuild)"
  mvn -B -q -DskipTests -Pdocker package jib:dockerBuild

  if [ "$PROFILE" = "integration" ]; then
    echo "==> Building flink-sql-runner JAR from ${SQL_RUNNER_REPO}"
    if [ ! -d "$SQL_RUNNER_REPO" ]; then
      echo "ERROR: flink-sql-runner repo not found at $SQL_RUNNER_REPO" >&2
      echo "       Set SQL_RUNNER_REPO to its checkout path, or clone:" >&2
      echo "       git clone https://github.com/acosom/flink-sql-runner.git \"$SQL_RUNNER_REPO\"" >&2
      exit 1
    fi
    (cd "$SQL_RUNNER_REPO" && mvn -B -q -DskipTests package)
    cp "$SQL_RUNNER_REPO/target/flink-sql-runner.jar" flink-jars/
  fi
fi

teardown() {
  # Dump cluster logs on integration so failures aren't opaque. The Channel-
  # became-inactive errors from JobController only tell us *that* the cluster
  # closed the connection, not why — the actual stack trace is on the JM/TM.
  if [ "$PROFILE" = "integration" ]; then
    echo "==> JobManager logs (last 200 lines)"
    docker compose -f ci/docker-compose.yaml --profile integration logs --tail=200 jobmanager 2>&1 || true
    echo "==> TaskManager logs (last 200 lines)"
    docker compose -f ci/docker-compose.yaml --profile integration logs --tail=200 taskmanager 2>&1 || true
  fi
  docker compose -f ci/docker-compose.yaml --profile "$PROFILE" \
    down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap teardown EXIT

if [ "$PROFILE" = "integration" ]; then
  echo "==> Booting cluster (redpanda + flink JM/TM) and waiting for healthchecks"
  docker compose -f ci/docker-compose.yaml --profile integration up \
    -d --wait redpanda jobmanager taskmanager

  echo "==> Uploading flink-sql-runner JAR to JobManager"
  # The runner uses the Flink REST /jars/upload endpoint to register the
  # pipeline JAR before submitting jobs. Mounting it into /opt/flink/usrlib
  # is not enough — that only adds it to the JM's classloader, not the
  # uploaded-jars list. So we POST it explicitly here.
  curl -fsS -X POST -H "Expect:" \
    -F "jarfile=@flink-jars/flink-sql-runner.jar" \
    http://localhost:8081/jars/upload
  echo
fi

echo "==> docker compose --profile $PROFILE up assert-runner"
docker compose -f ci/docker-compose.yaml --profile "$PROFILE" up \
  --abort-on-container-exit \
  --exit-code-from assert-runner \
  assert-runner

echo "==> Result file:"
cat result/result.txt
