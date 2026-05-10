package io.acosom.flink.assertrunner.report;

import org.junit.jupiter.api.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TextResultReporterTest {

    public static class PassingSuite {
        @org.junit.Test
        public void onePassingCase() {
        }
    }

    public static class FailingSuite {
        @org.junit.Test
        public void oneFailingCase() {
            org.junit.Assert.fail("expected boom");
        }
    }

    public static class TwoFailuresSuite {
        @org.junit.Test
        public void firstFails() {
            org.junit.Assert.fail("first");
        }

        @org.junit.Test
        public void secondFails() {
            org.junit.Assert.fail("second");
        }
    }

    @Test
    void writesSuiteHeaderOnTestStarted() {
        String output = capture(listener -> {
            listener.testStarted(Description.createSuiteDescription(PassingSuite.class));
        });

        assertThat(output)
                .contains("Test Suite:")
                .contains(PassingSuite.class.getName());
    }

    @Test
    void reportsPassWhenAllTestsSucceed() {
        String output = capture(listener -> {
            Result result = new JUnitCore().run(PassingSuite.class);
            listener.testRunFinished(result);
        });

        assertThat(output).contains("Result: Pass");
        assertThat(output).doesNotContain("Result: Fail");
    }

    @Test
    void reportsFailWhenSingleTestFails() {
        String output = capture(listener -> {
            Result result = new JUnitCore().run(FailingSuite.class);
            listener.testRunFinished(result);
        });

        assertThat(output).contains("Result: Fail");
        assertThat(output).contains("There was 1 failure:");
    }

    @Test
    void usesPluralPhrasingForMultipleFailures() {
        String output = capture(listener -> {
            Result result = new JUnitCore().run(TwoFailuresSuite.class);
            listener.testRunFinished(result);
        });

        assertThat(output).contains("There were 2 failures:");
    }

    @Test
    void writesEForFailureNotification() {
        String output = capture(listener -> listener.testFailure(
                new org.junit.runner.notification.Failure(
                        Description.createTestDescription(FailingSuite.class, "x"),
                        new AssertionError("nope"))));

        assertThat(output).isEqualTo("E");
    }

    @Test
    void writesIForIgnoredNotification() {
        String output = capture(listener -> listener.testIgnored(
                Description.createTestDescription(PassingSuite.class, "x")));

        assertThat(output).isEqualTo("I");
    }

    private static String capture(ListenerCall call) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream stream = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            TextResultReporter listener = new TextResultReporter(stream);
            call.run(listener);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface ListenerCall {
        void run(TextResultReporter listener);
    }
}
