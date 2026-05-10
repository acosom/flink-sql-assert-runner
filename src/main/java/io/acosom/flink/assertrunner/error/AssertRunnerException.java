package io.acosom.flink.assertrunner.error;

public class AssertRunnerException extends RuntimeException {

    public AssertRunnerException(String message) {
        super(message);
    }

    public AssertRunnerException(String message, Throwable cause) {
        super(message, cause);
    }
}
