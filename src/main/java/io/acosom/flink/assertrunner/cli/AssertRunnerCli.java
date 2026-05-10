package io.acosom.flink.assertrunner.cli;

import io.acosom.flink.assertrunner.config.ConfigLoader;
import io.acosom.flink.assertrunner.config.RunnerConfig;
import io.acosom.flink.assertrunner.integration.IntegrationContext;
import io.acosom.flink.assertrunner.integration.IntegrationSuite;
import io.acosom.flink.assertrunner.report.TextResultReporter;
import io.acosom.flink.assertrunner.unit.UnitSuiteRunner;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.Callable;

/**
 * Entry point for the assert runner. Reads configuration from environment
 * variables (or CLI flags) and dispatches to either the integration suite or
 * the unit suite.
 */
@Command(
        name = "flink-sql-assert-runner",
        mixinStandardHelpOptions = true,
        versionProvider = AssertRunnerCli.ManifestVersion.class,
        description = "Runs SQL-based assertion tests against Apache Flink jobs.")
public final class AssertRunnerCli implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(AssertRunnerCli.class);

    @Option(names = "--unit",
            description = "Run unit-style tests (compile + run user JUnit classes). "
                    + "Defaults from RUN_UNIT_TESTS env var.")
    Boolean unitMode;

    @Option(names = "--result-file",
            description = "Path where the result summary is written. "
                    + "Defaults from RESULT_FILE env var.")
    String resultFile;

    public static void main(String[] args) {
        int exit = new CommandLine(new AssertRunnerCli()).execute(args);
        System.exit(exit);
    }

    @Override
    public Integer call() throws Exception {
        RunnerConfig config = ConfigLoader.fromEnv(buildEnvOverlay());
        try (PrintStream out = new PrintStream(new java.io.FileOutputStream(config.getResultFile()))) {
            return config.isUnitMode() ? runUnit(config, out) : runIntegration(config, out);
        }
    }

    private java.util.Map<String, String> buildEnvOverlay() {
        java.util.Map<String, String> env = new java.util.HashMap<>(System.getenv());
        if (unitMode != null) {
            env.put("RUN_UNIT_TESTS", String.valueOf(unitMode));
        }
        if (resultFile != null) {
            env.put("RESULT_FILE", resultFile);
        }
        return env;
    }

    private int runUnit(RunnerConfig config, PrintStream out) {
        boolean ok = new UnitSuiteRunner(new File(config.getUnit().getJavaTestDir()), out).runAll();
        return ok ? 0 : 1;
    }

    private int runIntegration(RunnerConfig config, PrintStream out) {
        IntegrationContext.set(config.getIntegration());
        try {
            TextResultReporter reporter = new TextResultReporter(out);
            reporter.testStarted(Description.createTestDescription(
                    IntegrationSuite.class, IntegrationSuite.class.getSimpleName()));

            JUnitCore core = new JUnitCore();
            Result result = core.run(IntegrationSuite.class);
            reporter.testRunFinished(result);

            LOG.info("Integration tests {}", result.wasSuccessful() ? "passed" : "failed");
            return result.wasSuccessful() ? 0 : 1;
        } finally {
            IntegrationContext.clear();
        }
    }

    static final class ManifestVersion implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            String v = AssertRunnerCli.class.getPackage().getImplementationVersion();
            return new String[]{v == null ? "dev" : v};
        }
    }
}
