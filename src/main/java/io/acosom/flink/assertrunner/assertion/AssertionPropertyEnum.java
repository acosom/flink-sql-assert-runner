package io.acosom.flink.assertrunner.assertion;

public enum AssertionPropertyEnum {
    OUTPUT_COUNT("outputCount"),
    TIMEOUT_MS("timeoutMs"),
    MODE("mode");

    private final String propertyName;

    AssertionPropertyEnum(final String propertyName) {
        this.propertyName = propertyName;
    }

    public String getName() {
        return this.propertyName;
    }
}
