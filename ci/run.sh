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

if [ "$SKIP_BUILD" = "0" ]; then
  echo "==> Building assert-runner image (mvn -Pdocker jib:dockerBuild)"
  mvn -B -q -DskipTests -Pdocker jib:dockerBuild

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
  docker compose -f ci/docker-compose.yaml --profile "$PROFILE" \
    down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap teardown EXIT

echo "==> docker compose --profile $PROFILE up"
docker compose -f ci/docker-compose.yaml --profile "$PROFILE" up \
  --abort-on-container-exit \
  --exit-code-from assert-runner

echo "==> Result file:"
cat result/result.txt
