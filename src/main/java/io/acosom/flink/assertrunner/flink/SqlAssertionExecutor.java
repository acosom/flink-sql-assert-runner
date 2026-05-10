package io.acosom.flink.assertrunner.flink;

import io.acosom.flink.assertrunner.assertion.AssertionModesEnum;
import io.acosom.flink.assertrunner.assertion.AssertionProperties;
import io.acosom.flink.assertrunner.assertion.AssertionSpecParser;
import io.acosom.flink.assertrunner.error.AssertionFailureException;
import io.acosom.flink.assertrunner.error.FlinkJobException;
import io.acosom.flink.assertrunner.template.EnvVarTemplating;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Runs a SQL assertion file against a remote Flink session: parses the SQL,
 * executes preamble statements, evaluates the trailing SELECT, and validates
 * the result row count against the inline assertion mode.
 */
public final class SqlAssertionExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SqlAssertionExecutor.class);

    private final URI jobmanagerUri;
    private final String userJarPath;
    private final Long globalSuccessTimeoutMs;

    public SqlAssertionExecutor(String jobmanagerUrl, String userJarPath, Long globalSuccessTimeoutMs) {
        this.jobmanagerUri = URI.create(jobmanagerUrl);
        this.userJarPath = userJarPath;
        this.globalSuccessTimeoutMs = globalSuccessTimeoutMs;
    }

    public void run(File sqlFile) {
        String script = loadScript(sqlFile);
        AssertionProperties spec = AssertionSpecParser.parse(script);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                jobmanagerUri.getHost(), jobmanagerUri.getPort(), userJarPath);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        List<SqlNode> statements = parseStatements(script, sqlFile);
        executePreamble(tableEnv, statements);

        List<Row> resultRows = collectFinalSelect(tableEnv, statements, spec);
        assertResult(spec, resultRows, sqlFile);
    }

    private String loadScript(File sqlFile) {
        try {
            String script = FileUtils.readFileUtf8(sqlFile);
            return EnvVarTemplating.apply(script, System.getenv());
        } catch (IOException e) {
            throw new FlinkJobException("Failed to read SQL assertion file " + sqlFile, e);
        }
    }

    private List<SqlNode> parseStatements(String script, File sqlFile) {
        SqlParser.Config config = SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withConformance(FlinkSqlConformance.DEFAULT)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
        try {
            return SqlParser.create(script, config).parseStmtList().getList();
        } catch (SqlParseException e) {
            throw new FlinkJobException("Failed to parse SQL file " + sqlFile, e);
        }
    }

    private void executePreamble(StreamTableEnvironment tableEnv, List<SqlNode> statements) {
        for (int i = 0; i < statements.size() - 1; i++) {
            try {
                tableEnv.executeSql(statements.get(i).toString());
            } catch (Throwable t) {
                LOG.warn("Preamble statement #{} failed: {}", i, t.getMessage(), t);
            }
        }
    }

    private List<Row> collectFinalSelect(StreamTableEnvironment tableEnv,
                                          List<SqlNode> statements,
                                          AssertionProperties spec) {
        List<Row> rows = new ArrayList<>();
        if (statements.isEmpty()) {
            return rows;
        }

        SqlNode finalStmt = statements.get(statements.size() - 1);
        var tableResult = tableEnv.executeSql(finalStmt.toString());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit(() -> {
            tableResult.collect().forEachRemaining(rows::add);
            return null;
        });

        long timeoutMs = Optional.ofNullable(globalSuccessTimeoutMs).orElse(spec.getTimeoutMs());
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception expected) {
            future.cancel(true);
        } finally {
            executor.shutdownNow();
        }
        return rows;
    }

    private void assertResult(AssertionProperties spec, List<Row> rows, File sqlFile) {
        String mode = spec.getMode();
        if (AssertionModesEnum.NEGATIVE.getName().equals(mode)) {
            if (!rows.isEmpty()) {
                throw new AssertionFailureException(String.format(
                        "Assertion file %s expected no rows but got:%n%s",
                        sqlFile.getName(), rowsAsString(rows)));
            }
        } else if (AssertionModesEnum.POSITIVE.getName().equals(mode)) {
            if (rows.size() != spec.getOutputCount()) {
                throw new AssertionFailureException(String.format(
                        "Assertion file %s expected %d rows but got %d:%n%s",
                        sqlFile.getName(), spec.getOutputCount(), rows.size(), rowsAsString(rows)));
            }
        }
    }

    private static String rowsAsString(List<Row> rows) {
        return rows.stream().map(Object::toString).collect(Collectors.joining("\n"));
    }
}
