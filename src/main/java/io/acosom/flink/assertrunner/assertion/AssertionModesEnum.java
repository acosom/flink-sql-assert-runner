package io.acosom.flink.assertrunner.assertion;

public enum AssertionModesEnum {
    POSITIVE("positive"),
    NEGATIVE("negative");

    private final String modeName;

    AssertionModesEnum(final String modeName) {
        this.modeName = modeName;
    }

    public String getName() {
        return this.modeName;
    }
}
