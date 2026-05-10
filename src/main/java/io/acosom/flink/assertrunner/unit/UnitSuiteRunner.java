package io.acosom.flink.assertrunner.unit;

import io.acosom.flink.assertrunner.error.AssertRunnerException;
import io.acosom.flink.assertrunner.report.TextResultReporter;
import io.acosom.flink.assertrunner.unit.compile.JavaCompilerUtil;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;

/**
 * Compiles every {@code .java} file under the configured directory and runs
 * each as a JUnit 4 test class. Results are streamed to a
 * {@link TextResultReporter}.
 */
public final class UnitSuiteRunner {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSuiteRunner.class);

    private final File javaTestDir;
    private final PrintStream output;

    public UnitSuiteRunner(File javaTestDir, PrintStream output) {
        this.javaTestDir = javaTestDir;
        this.output = output;
    }

    public boolean runAll() {
        if (!javaTestDir.isDirectory()) {
            throw new AssertRunnerException("Java test directory is not a directory: " + javaTestDir);
        }
        File[] testFiles = javaTestDir.listFiles();
        if (testFiles == null) {
            return true;
        }

        TextResultReporter reporter = new TextResultReporter(output);
        JUnitCore core = new JUnitCore();
        boolean allPassed = true;

        for (File testFile : testFiles) {
            if (!testFile.isFile()) {
                continue;
            }
            Class<?> testClass = JavaCompilerUtil.compile(testFile);
            if (testClass == null) {
                throw new AssertRunnerException("Failed to compile test class from " + testFile);
            }

            reporter.testStarted(Description.createTestDescription(testClass, testClass.getSimpleName()));
            LOG.info("Running test suite {}", testClass.getSimpleName());
            Result result = core.run(testClass);
            reporter.testRunFinished(result);

            if (result.wasSuccessful()) {
                LOG.info("Test suite {} passed", testClass.getSimpleName());
            } else {
                LOG.error("Test suite {} failed", testClass.getSimpleName());
                allPassed = false;
            }
        }
        return allPassed;
    }
}
