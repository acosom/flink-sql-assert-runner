package io.acosom.flink.assertrunner.unit;

import io.acosom.flink.assertrunner.error.AssertRunnerException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for Flink SQL unit tests. Sets up an in-process
 * {@link StreamTableEnvironment} backed by a Paimon catalog rooted at a
 * scratch directory, then runs the user-supplied SQL script (its filename is
 * returned by {@link #getScriptName()}).
 *
 * <p>The script is rewritten in-flight: {@code ADD JAR} statements are
 * stripped, {@code CREATE TABLE ... WITH (...)} loses its connector config
 * (so the test's Paimon catalog backs them), and {@code CREATE VIEW} becomes
 * {@code CREATE TEMPORARY VIEW}.</p>
 */
public abstract class FlinkSqlTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlTestCase.class);

    private static final String DEFAULT_SQL_DIR = "/opt/flink/sql";

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    private Path tempSpace;

    @Before
    public void setUp() {
        cleanupTempSpace();
        this.tempSpace = createTempSpace();

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(1000);

        tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");

        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon WITH ('type'='paimon','warehouse'='%s');",
                tempSpace.toString()));
        tEnv.executeSql("USE CATALOG paimon;");

        SqlNodeList statements = parseStatements(new File(sqlScriptDir(), getScriptName()));
        statements.forEach(stmt -> tEnv.executeSql(stmt.toString()));
    }

    @After
    public void teardown() {
        cleanupTempSpace();
    }

    public abstract String getScriptName();

    protected List<Row> selectRowsWithTimeout(String table, Integer expectedSize) {
        return selectRowsWithTimeout(table, expectedSize, 15);
    }

    protected List<Row> selectRowsWithTimeout(String table, Integer expectedSize, Integer timeoutInSeconds) {
        var tableResult = tEnv.executeSql(String.format("SELECT * FROM %s;", table));
        List<Row> collected = new ArrayList<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            CloseableIterator<Row> it = tableResult.collect();
            while (collected.size() < expectedSize && it.hasNext()) {
                collected.add(it.next());
            }
        });

        runWithTimeout(future, timeoutInSeconds, executor);
        return collected;
    }

    protected List<Row> selectAllRowsWithTimeout(String table, Integer timeoutInSeconds) {
        var tableResult = tEnv.executeSql(String.format("SELECT * FROM %s;", table));
        List<Row> collected = new ArrayList<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(
                () -> tableResult.collect().forEachRemaining(collected::add));

        runWithTimeout(future, timeoutInSeconds, executor);
        return collected;
    }

    private static void runWithTimeout(Future<?> future, Integer timeoutInSeconds, ExecutorService executor) {
        try {
            future.get(timeoutInSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
        } finally {
            executor.shutdownNow();
        }
    }

    private SqlNodeList parseStatements(File scriptFile) {
        String script;
        try {
            script = FileUtils.readFileUtf8(scriptFile);
        } catch (IOException e) {
            throw new AssertRunnerException("Failed to read SQL file " + scriptFile, e);
        }
        script = stripAddJar(script);
        script = stripTableConnectorOptions(script);
        script = makeViewsTemporary(script);

        SqlParser parser = SqlParser.create(script, SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withConformance(FlinkSqlConformance.DEFAULT)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256));
        try {
            return parser.parseStmtList();
        } catch (SqlParseException e) {
            throw new AssertRunnerException("Failed to parse SQL file " + scriptFile, e);
        }
    }

    private static String makeViewsTemporary(String script) {
        return script.replaceAll("CREATE VIEW", Matcher.quoteReplacement("CREATE TEMPORARY VIEW"));
    }

    private static String stripAddJar(String script) {
        return script.replaceAll("ADD\\s+JAR\\s+'[^']*'\\s*;", "");
    }

    private static String stripTableConnectorOptions(String script) {
        Pattern createTable = Pattern.compile("CREATE TABLE(?:[^';]|'[^']*')*?;");
        Matcher matcher = createTable.matcher(script);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String stripped = matcher.group().replaceAll("WITH \\([\\s\\S]*?\\)", ";");
            matcher.appendReplacement(result, Matcher.quoteReplacement(stripped));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private static Path createTempSpace() {
        try {
            Path tempPath = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
            if (!Files.exists(tempPath)) {
                Files.createDirectories(tempPath);
            }
            return tempPath;
        } catch (IOException e) {
            throw new AssertRunnerException("Failed to create scratch directory", e);
        }
    }

    private void cleanupTempSpace() {
        if (tempSpace != null && Files.exists(tempSpace)) {
            deleteRecursively(tempSpace.toFile());
        }
    }

    private static void deleteRecursively(File target) {
        File[] children = target.listFiles();
        if (children != null) {
            for (File child : children) {
                try {
                    if (child.isDirectory()) {
                        deleteRecursively(child);
                    } else {
                        Files.delete(child.toPath());
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to delete {}: {}", child, e.getMessage());
                }
            }
        }
        try {
            Files.delete(target.toPath());
        } catch (IOException e) {
            LOG.warn("Failed to delete {}: {}", target, e.getMessage());
        }
    }

    private static String sqlScriptDir() {
        return Optional.ofNullable(System.getenv("UNIT_TEST_SQL_DIR")).orElse(DEFAULT_SQL_DIR);
    }
}
