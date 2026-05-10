package io.acosom.flink.assertrunner.error;

public class FlinkJobException extends AssertRunnerException {

    public FlinkJobException(String message) {
        super(message);
    }

    public FlinkJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
