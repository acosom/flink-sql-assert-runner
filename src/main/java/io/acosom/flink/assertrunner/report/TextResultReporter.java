package io.acosom.flink.assertrunner.report;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.PrintStream;
import java.util.List;

/**
 * JUnit 4 RunListener that emits a human-readable plain-text summary suitable
 * for tailing as a CI log or saving as the runner's result file.
 */
public final class TextResultReporter extends RunListener {

    private final PrintStream writer;

    public TextResultReporter(PrintStream writer) {
        this.writer = writer;
    }

    @Override
    public void testRunFinished(Result result) {
        printResult(result);
        printFailures(result);
        writer.println();
    }

    @Override
    public void testStarted(Description description) {
        writer.println("Test Suite: " + description.getClassName() + ": ");
    }

    @Override
    public void testFailure(Failure failure) {
        writer.append('E');
    }

    @Override
    public void testIgnored(Description description) {
        writer.append('I');
    }

    private void printResult(Result result) {
        writer.println("\t- Result: " + (result.wasSuccessful() ? "Pass" : "Fail"));
    }

    private void printFailures(Result result) {
        List<Failure> failures = result.getFailures();
        if (failures.isEmpty()) {
            return;
        }
        writer.println(failures.size() == 1
                ? "\t- Cause: There was 1 failure:"
                : "\t- Cause: There were " + failures.size() + " failures:");
        int i = 1;
        for (Failure each : failures) {
            printFailure(each, "\t\t" + i++);
        }
        writer.println();
    }

    private void printFailure(Failure failure, String prefix) {
        writer.println(prefix + ") " + failure.getTestHeader());
        writer.print("\t\t" + String.join("\n\t", failure.getTrimmedTrace().split("\n")));
        writer.print("");
    }
}
