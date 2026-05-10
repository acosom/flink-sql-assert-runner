# flink-sql-assert-runner

A test harness for [Apache Flink](https://flink.apache.org/) SQL pipelines. Define
test scenarios and assertions as plain SQL files; the runner publishes Avro
fixtures to Kafka, deploys your Flink SQL job, runs assertion queries against
the resulting state, and reports pass/fail.

Two complementary modes:

- **Integration tests** — exercise the full pipeline end-to-end (Kafka source →
  Flink job → Kafka sink) with assertions written as SQL `SELECT` statements
  against the live result topics.
- **Unit tests** — exercise a Flink SQL script in isolation, in-process, using
  an [Apache Paimon](https://paimon.apache.org/) catalog as a test backend so
  no external Kafka or Schema Registry is required.

License: Apache 2.0.

---

## Table of contents

- [How it works](#how-it-works)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
- [Configuration reference](#configuration-reference)
- [Integration tests](#integration-tests)
- [Unit tests](#unit-tests)
- [Building from source](#building-from-source)
- [Project structure](#project-structure)
- [Contributing](#contributing)

---

## How it works

The runner ships as a fat JAR (`flink-sql-assert-runner.jar`) with a
[Picocli](https://picocli.info/) CLI. Configuration is read from environment
variables (and optionally overridden by CLI flags), then dispatched to either
the integration or the unit test path.

### Integration mode

For each scenario folder under `INTEGRATION_TEST_DATA_DIR`:

1. **Cancel any running Flink jobs** so each scenario starts clean.
2. **Purge** all configured `INTEGRATION_OUTPUT_TOPICS` and per-scenario input
   topics (delete + recreate) so consumer offsets and stale data don't leak.
3. **Publish** the JSON fixtures from `input/<topic>.json` to Kafka, encoded
   as Avro using the schema in `input/schema/<topic>.json`.
4. **Submit** the Flink SQL job. The runner expects a job JAR uploaded to the
   JobManager (typically a SQL-runner that consumes `INTEGRATION_TEST_JOB_SQL_FILE`)
   and an entrypoint class given by `INTEGRATION_FLINK_JOB_ENTRYPOINT_CLASS`.
5. **Validate**:
   - If `output/<topic>.json` files exist, the runner consumes from each topic
     and checks at least one expected message arrives within a timeout.
   - If `sqlAssertions/*.sql` files exist, each one is run as a Flink job. The
     last `SELECT` in the file produces rows that are validated against the
     inline assertion spec.

### Unit mode

Java test classes under `UNIT_TEST_JAVA_DIR` are compiled at runtime and
executed via JUnit 4. Each test class extends `FlinkSqlTestCase` and points at
a SQL script in `UNIT_TEST_SQL_DIR`. The script's connector configuration
(`WITH (...)` clauses on `CREATE TABLE`) is stripped so backing storage is
provided by an in-process Paimon catalog — no Kafka or external services
required.

### Architecture

```
                ┌─────────────────────────────────────────────┐
                │  AssertRunnerCli (Picocli main)             │
                │           ConfigLoader → RunnerConfig       │
                └─────────────┬───────────────────┬───────────┘
                              │ unit              │ integration
                              ▼                   ▼
              ┌────────────────────┐   ┌────────────────────────┐
              │ UnitSuiteRunner    │   │ IntegrationSuite       │
              │ JavaCompilerUtil   │   │ (JUnit 4 test class)   │
              │ FlinkSqlTestCase   │   └─────────┬──────────────┘
              └────────────────────┘             │
                                       ┌─────────┼─────────┐
                                       ▼         ▼         ▼
                                ┌──────────┐ ┌────────┐ ┌────────────┐
                                │ kafka.*  │ │ flink. │ │ assertion. │
                                │ Loader   │ │ Job    │ │ SpecParser │
                                │ Verifier │ │ Cont.  │ │ Templating │
                                │ TopicAd. │ │ SqlExec│ │            │
                                └──────────┘ └────────┘ └────────────┘
```

---

## Quick start (Docker Compose)

A reference Compose file is included that boots Kafka, an Apicurio Schema
Registry (Confluent-compatible), Kafka UI, and a Flink session cluster.

1. Drop your Flink SQL job JAR (the one that reads SQL files and submits them
   as Flink jobs — bring your own, e.g. one built from
   [`flink-sql-runner`](https://github.com/getindata/flink-sql-runner) or your
   own implementation) into `./flink-jars/`.

2. Adjust `integration.env` for your scenario (Kafka topics, Flink job entry
   point class, etc.).

3. Boot the supporting services:

    ```bash
    docker compose up -d kafka_broker apicurio jobmanager taskmanager
    ```

4. Run the assert runner:

    ```bash
    mvn package -DskipTests
    docker compose run --rm --build flink-sql-assert-runner
    ```

   Or run the JAR directly:

    ```bash
    set -a; source integration.env; set +a
    java -jar target/flink-sql-assert-runner.jar
    ```

---

## Configuration reference

All settings come from environment variables. The CLI also accepts
`--unit` / `--result-file` overrides — run with `--help` for the full list.

### Common

| Variable          | Required | Purpose                                                  |
|-------------------|----------|----------------------------------------------------------|
| `RESULT_FILE`     | yes      | Path where the human-readable result summary is written. |
| `RUN_UNIT_TESTS`  | no       | `true` to run unit tests; defaults to `false`.           |

### Integration

| Variable                                 | Required | Purpose                                                        |
|------------------------------------------|----------|----------------------------------------------------------------|
| `INTEGRATION_TEST_DATA_DIR`              | yes      | Directory containing one folder per scenario.                  |
| `INTEGRATION_KAFKA_SERVER`               | yes      | Bootstrap servers, e.g. `localhost:9092`.                      |
| `INTEGRATION_SCHEMA_REGISTRY_URL`        | yes      | Confluent-compatible Schema Registry URL.                      |
| `INTEGRATION_FLINK_JOBMANAGER_SERVER`    | yes      | Flink JobManager REST URL, e.g. `http://localhost:8081`.       |
| `INTEGRATION_TEST_JOB_SQL_FILE`          | yes      | SQL file to deploy (relative to the runner JAR's `/opt/flink/sql`). |
| `INTEGRATION_FLINK_JOB_ENTRYPOINT_CLASS` | yes      | Main class of the Flink job JAR you uploaded.                  |
| `INTEGRATION_OUTPUT_TOPICS`              | no       | Comma-separated list of sink topics to purge before each run.  |
| `INTEGRATION_TEST_SUCCESS_TIMEOUT_MS`    | no       | Global timeout for a SQL assertion's `SELECT`. Default `10000`.|

### Unit

| Variable                     | Required | Default          | Purpose                                              |
|------------------------------|----------|------------------|------------------------------------------------------|
| `UNIT_TEST_JAVA_DIR`         | yes      | `/app/test`      | Directory containing `*.java` test classes.          |
| `UNIT_TEST_SQL_DIR`          | no       | `/opt/flink/sql` | Directory the test classes reference for SQL scripts.|
| `UNIT_TEST_INPUT_EVENTS_DIR` | no       | `/app/input`     | Directory for fixture data used by tests.            |

---

## Integration tests

### Scenario layout

```
📂 integration
├── 📂 scenario-1
│   ├── 📂 input
│   │   ├── 📂 schema
│   │   │   └── 📄 input_topic.json   # Avro schema (.avsc-style JSON)
│   │   └── 📄 input_topic.json       # array of records to publish
│   ├── 📂 output                      # optional: expected sink topic data
│   │   └── 📄 output_topic.json
│   └── 📂 sqlAssertions               # optional: SQL-based assertions
│       └── 📄 some_check.sql
└── 📂 scenario-2
    └── …
```

### Assertion files

Each `.sql` file in `sqlAssertions/` is a Flink SQL script with **two
extensions** on top of standard Flink SQL:

**Environment variable substitution.** `@@VAR_NAME@@` placeholders are replaced
with the corresponding environment variable value at runtime. Single quotes in
the value are escaped to `''` so they're safe inside SQL string literals.

**Inline assertion spec.** Anywhere in the file (typically as a SQL comment),
include `key:value` tokens to control validation:

| Token         | Default    | Meaning                                                                  |
|---------------|------------|--------------------------------------------------------------------------|
| `mode`        | `negative` | `positive` = expect an exact row count; `negative` = expect zero rows.   |
| `outputCount` | `0`        | Number of rows expected in `positive` mode.                              |
| `timeoutMs`   | `10000`    | How long to collect rows before evaluating (overridden by `INTEGRATION_TEST_SUCCESS_TIMEOUT_MS` if set). |

**The last statement in the file must be a `SELECT`** — that's what produces
the rows the runner validates.

#### Negative example

A negative assertion passes when the `SELECT` returns *no* rows. Use it to
assert "this bad state never appears":

```sql
CREATE TABLE OUTPUT_SOURCE (
    id   STRING NOT NULL,
    name STRING NOT NULL
) WITH (
    'connector'    = 'kafka',
    'topic'        = 'output_topic',
    'properties.bootstrap.servers' = '@@INTEGRATION_KAFKA_SERVER@@',
    'properties.group.id' = 'check-1',
    'scan.startup.mode'   = 'earliest-offset',
    'value.format'        = 'avro-confluent',
    'value.avro-confluent.url' = '@@INTEGRATION_SCHEMA_REGISTRY_URL@@'
);

-- mode:negative
SELECT * FROM OUTPUT_SOURCE WHERE id NOT IN ('1', '2', '3');
```

#### Positive example

A positive assertion passes when the row count matches `outputCount`:

```sql
-- … same CREATE TABLE …

SELECT * FROM OUTPUT_SOURCE; -- mode:positive outputCount:2
```

---

## Unit tests

### Layout

```
📂 unit
├── 📂 script
│   └── 📄 my-job.sql        # the Flink SQL job under test
└── 📂 test
    └── 📄 MyJobTest.java     # JUnit 4 test class
```

### Example test

```java
import org.junit.Assert;
import org.junit.Test;
import io.acosom.flink.assertrunner.unit.FlinkSqlTestCase;

public class MyJobTest extends FlinkSqlTestCase {

    @Override
    public String getScriptName() {
        return "my-job.sql";
    }

    @Test
    public void filtersToActiveRows() {
        tEnv.executeSql("INSERT INTO INPUT_TABLE VALUES ('1', 'active'), ('2', 'inactive')");

        var rows = selectRowsWithTimeout("OUTPUT_TABLE", 1);

        Assert.assertEquals(1, rows.size());
    }
}
```

What `FlinkSqlTestCase` does for you on `@Before`:

1. Creates a fresh Paimon catalog rooted at a temp directory.
2. Reads your SQL script and rewrites it for in-process execution: strips
   `ADD JAR` statements, replaces `WITH (...)` connector configs with empty
   ones (so Paimon backs the tables), and converts `CREATE VIEW` →
   `CREATE TEMPORARY VIEW`.
3. Executes the rewritten statements against the test `StreamTableEnvironment`.

You then write `INSERT` statements for fixture data and use the helpers
(`selectRowsWithTimeout`, `selectAllRowsWithTimeout`) to assert on results.

---

## Building from source

### Prerequisites

- JDK 17
- Maven 3.8+
- Docker (only required for integration tests against the testcontainers
  Kafka + Schema Registry suite — `mvn verify`)

### Commands

```bash
# Compile, run unit tests
mvn test

# Compile, run unit + integration tests (slower, needs Docker)
mvn verify

# Build the fat JAR
mvn package

# Build a Docker image
mvn jib:dockerBuild
```

The fat JAR lands at `target/flink-sql-assert-runner.jar`.

### Running the test suite

The repository's own test suite has two tiers:

| Tier               | What it covers                                                | Runs in   |
|--------------------|---------------------------------------------------------------|-----------|
| Unit (`*Test.java`) | Pure-logic classes: parsing, templating, config, reporting.   | `mvn test`|
| IT (`*IT.java`)    | Kafka end-to-end via testcontainers (Kafka + Schema Registry).| `mvn verify` |

If you're on Docker 29+ and tests fail with `client version 1.32 is too old`,
the failsafe configuration already sets `-Dapi.version=1.45` to work around the
bundled docker-java client.

---

## Project structure

```
src/main/java/io/acosom/flink/assertrunner/
├── cli/             # Picocli entry point
├── config/          # RunnerConfig record + ConfigLoader
├── assertion/       # AssertionSpec, parser, modes/fields enums
├── template/        # @@VAR@@ environment templating
├── flink/           # JobController + SqlAssertionExecutor
├── kafka/           # KafkaFixtureLoader, KafkaOutputVerifier, TopicAdmin
├── integration/     # IntegrationSuite (drives a scenario)
├── unit/            # FlinkSqlTestCase, UnitSuiteRunner, JavaCompilerUtil
├── report/          # TextResultReporter (writes the result file)
└── error/           # Typed exceptions
```

---

## Contributing

PRs welcome. Quick rules:

- Run `mvn test` before pushing. If your change touches Kafka I/O,
  also run `mvn verify`.
- Keep public API changes deliberate — this project is pre-1.0 but downstream
  Flink SQL test suites get tied to method signatures quickly.
- New behavior should land with a test pinning it.

For larger changes, open an issue first to discuss direction.
