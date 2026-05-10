package io.acosom.flink.assertrunner.e2e;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Minimal Flink SQL runner used as the uploaded job JAR in
 * {@code FlinkJobControllerIT}. Submits a self-contained datagen → blackhole
 * pipeline that runs forever (so the IT can verify cancel) and has no external
 * dependencies (no Kafka, no Schema Registry, no user-supplied SQL file).
 *
 * <p>Program args are ignored on purpose: {@code JobController} passes a SQL
 * file path that this runner doesn't need, but the unused arg keeps the test
 * matched against production wiring.</p>
 */
public final class MinimalSqlRunner {

    private MinimalSqlRunner() {
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE src (n BIGINT) WITH ('connector'='datagen', 'rows-per-second'='1')");
        tableEnv.executeSql(
                "CREATE TABLE sink (n BIGINT) WITH ('connector'='blackhole')");
        tableEnv.executeSql("INSERT INTO sink SELECT n FROM src");
    }
}
