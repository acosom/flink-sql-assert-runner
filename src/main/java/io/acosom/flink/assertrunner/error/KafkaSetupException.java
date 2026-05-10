package io.acosom.flink.assertrunner.error;

public class KafkaSetupException extends AssertRunnerException {

    public KafkaSetupException(String message) {
        super(message);
    }

    public KafkaSetupException(String message, Throwable cause) {
        super(message, cause);
    }
}
