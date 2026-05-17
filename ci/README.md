# `ci/` — CI plumbing

Everything in this directory exists to run the assert runner end-to-end
against a real Kafka + real Flink, in containers, with a single command.
Both GitHub Actions (`.github/workflows/ci.yml`) and local repros use the
same entry point.

If you just want to use the assert runner against your own cluster, you
do **not** need any of this — see the main [README](../README.md). This
directory is the test harness for the runner itself.

## Files

| File                  | Purpose                                                                                                       |
|-----------------------|---------------------------------------------------------------------------------------------------------------|
| `run.sh`              | The entry point. Takes `unit` or `integration` as its argument.                                               |
| `docker-compose.yaml` | The compose stack: Redpanda + Flink JM + Flink TM + the assert-runner image. Two profiles: `unit`, `integration`. |
| `integration.env`     | Env file loaded by the assert-runner container when running `integration`. Defaults are host-side endpoints — the compose service overrides the network-related ones with cluster DNS names. |
| `unit.env`            | Env file for `unit` profile. Sets `RUN_UNIT_TESTS=true`. No Kafka/Flink needed.                               |
| `entrypoint.sh`       | Container entrypoint baked into the jib image. Validates required env, then runs the assert-runner JAR.       |

## Running locally

```bash
# Unit tests — in-process Flink + Paimon, no external services
./ci/run.sh unit

# Integration tests — full pipeline against the containerized stack
./ci/run.sh integration
```

The script exits with the assert-runner's exit code. The plain-text
result file lands in `result/result.txt`.

`SKIP_BUILD=1 ./ci/run.sh integration` reuses the already-built image
and SQL runner JAR — useful for iterating on the compose stack or the
script itself without rebuilding Maven artifacts.

## What happens when you run `integration`

### 1. Build phase

`run.sh` does two Maven builds:

1. `mvn -Pdocker package jib:dockerBuild` — produces the assert-runner
   container image (`flink-sql-assert-runner:<version>`) and loads it
   into the local Docker daemon. Gated behind the `docker` profile so
   regular `mvn package` users don't pull in jib.
2. `cd $SQL_RUNNER_REPO && mvn package` — builds the
   [`flink-sql-runner`](https://github.com/acosom/flink-sql-runner)
   uber-JAR from a sibling checkout, then `cp`s it into
   `flink-jars/flink-sql-runner.jar`. `SQL_RUNNER_REPO` defaults to
   `../sql-runner`; override the env var if your checkout lives
   elsewhere. CI clones it as part of the workflow.

### 2. Cluster boot

```bash
docker compose --profile integration up -d --wait redpanda jobmanager taskmanager
```

`--wait` blocks until each service's healthcheck passes:

- **Redpanda** — single container providing Kafka broker (port `19092`
  on host, `9092` inside the compose network) and Confluent-compatible
  Schema Registry (`18081` / `8081`). Replaces the
  Zookeeper/cp-server/Apicurio trio with one process; starts in ~2-3s.
- **Flink JobManager** — session mode, REST exposed on `8081`. Mounts
  `../flink-jars/` to `/opt/flink/usrlib` so the JM has the SQL runner
  on its classpath, and `../src/main/resources/data/sql-script/` to
  `/opt/flink/sql` so the pipeline SQL file is reachable by path. Also
  loads `ci/integration.env` so the cluster-side SQL runner can resolve
  `@@VAR@@` placeholders in the pipeline SQL.
- **Flink TaskManager** — joins the JM. 2 slots.

### 3. JAR upload

```bash
curl -X POST -F "jarfile=@flink-jars/flink-sql-runner.jar" \
  http://localhost:8081/jars/upload
```

The `JobController` inside the assert runner submits jobs by looking up
an *uploaded* JAR via the REST `/jars` endpoint. Mounting the JAR into
`/opt/flink/usrlib` only adds it to the JM's classloader — it does
**not** make it visible in the `/jars` list. We POST it explicitly here.

### 4. Assert runner

```bash
docker compose --profile integration up \
  --abort-on-container-exit --exit-code-from assert-runner assert-runner
```

The assert-runner image launches with `ci/integration.env` plus the
in-network env overrides set in `docker-compose.yaml`. Inside the
container, the runner drives each scenario under
`src/main/resources/integration/`:

1. **Cancel** any jobs still running on the cluster from prior scenarios.
2. **Purge** the configured output topics (`INTEGRATION_OUTPUT_TOPICS`)
   and the per-scenario input topics (filenames in `input/`). Each is
   deleted and recreated to guarantee a clean slate. **Test topics must
   not share names with production topics** (see callout in main
   README's Configuration reference).
3. **Publish** the JSON fixtures from each scenario's `input/<topic>.json`
   to Kafka, encoded as Avro using the schema in `input/schema/<topic>.json`.
4. **Submit the pipeline job** via the JM REST API (`POST /jars/:id/run`,
   referencing the JAR we uploaded in step 3). The pipeline JAR's main
   class (`io.acosom.flink.sqlrunner.SqlRunnerMain`) reads the SQL file
   from `/opt/flink/sql/`, resolves `@@VAR@@` placeholders against the
   JM pod's env, and `tEnv.executeSql(...)` each statement. For a
   streaming pipeline this kicks off the source/sink topology and the
   job stays running.
5. **Validate** in one or both of two ways:
   - **Output snapshot** — if `output/<topic>.json` is present, consume
     from that topic for up to ~30s and confirm every expected record
     appeared.
   - **SQL assertion** — if `sqlAssertions/*.sql` is present, the
     runner submits each as its own Flink job against the cluster (via
     `createRemoteEnvironment` from the assert-runner JVM), collects
     the rows of the final `SELECT`, and decides pass/fail by row
     count according to the inline `mode:positive|negative` /
     `outputCount:N` spec.
6. **Write `result.txt`** to the mounted `../result/` directory and
   exit with `0` (all green) or `1` (at least one failure).

`--abort-on-container-exit --exit-code-from assert-runner` makes compose
tear down the rest of the stack as soon as the runner exits, with the
runner's status code propagated up to the script.

### 5. Teardown

`trap` in `run.sh` always calls `docker compose down --volumes
--remove-orphans` before exiting, even on error. CI uploads
`result/result.txt` as a workflow artifact for inspection.

## What happens when you run `unit`

Much simpler — no cluster involved.

1. Same image build as above (jib).
2. `docker compose --profile unit up` brings up *only* the
   `assert-runner` container. Redpanda, JM, and TM all have
   `profiles: [integration]` so they don't start. `depends_on` entries
   on the assert-runner use `required: false`, so the deps are
   skipped when the depended service is excluded by profile.
3. The container loads `ci/unit.env` (`RUN_UNIT_TESTS=true`) plus the
   compose overrides pointing `UNIT_TEST_SQL_DIR` and
   `UNIT_TEST_JAVA_DIR` at the volume-mounted
   `examples/unit-tests/unit/{script,test}`.
4. The runner compiles each `*.java` test file at runtime via the JDK
   `javac` API (which is why the image base is `eclipse-temurin:17-jdk`
   — a JRE would return `null` from `ToolProvider.getSystemJavaCompiler()`
   and crash), loads them into a fresh classloader, and runs them via
   JUnit 4. Each test extends `FlinkSqlTestCase`, which sets up an
   in-process `StreamTableEnvironment` backed by an
   [Apache Paimon](https://paimon.apache.org/) catalog rooted at a temp
   directory. See the main README's [Unit testing](../README.md#unit-testing)
   section for the catalog-substitution mechanism.
5. `result.txt` is written the same way.

## Why session mode?

The compose stack runs Flink in **session mode** — one shared JM + TMs,
multiple jobs submittable via REST. This is a CI convenience: only one
pipeline runs per CI invocation, so there's nothing for env vars to
leak into.

**Don't model your production deployment on this**. In real Kubernetes,
use the
[Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
and give each pipeline its own `FlinkDeployment` (application mode).
Env on the deployment's `podTemplate` is then naturally scoped per
pipeline. Point the assert runner at that FlinkDeployment's JobManager
REST URL via `INTEGRATION_FLINK_JOBMANAGER_SERVER` — no other changes.

## Troubleshooting

- **`jobmanager is unhealthy`** — the healthcheck probes port `8081`
  via `bash`'s `/dev/tcp`. If you see this on a clean run, the JM is
  taking longer than the configured `start_period` (60s). Bump it in
  `docker-compose.yaml`.
- **`No JARs uploaded to JobManager`** — the `curl` upload step in
  `run.sh` failed silently, or `flink-jars/flink-sql-runner.jar` isn't
  present. Either build the JAR manually (`cd ../sql-runner && mvn
  package`) and copy it in, or re-run the script with `SKIP_BUILD=0`
  (the default) so it stages it.
- **`Failed to compile file … javax.tools.JavaCompiler … is null`** —
  the image base is a JRE instead of a JDK. Check the `<image>` element
  in the `docker` profile of the parent `pom.xml`.
- **`NoClassDefFoundError: org/apache/hadoop/conf/Configuration`** —
  Paimon needs Hadoop on the classpath. Make sure `hadoop-client-api`
  and `hadoop-client-runtime` are in the pom at compile scope (not
  test scope).
