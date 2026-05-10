package io.acosom.flink.assertrunner.assertion;

public class AssertionProperties {

    private final String mode;
    private final Integer outputCount;
    private final Long timeoutMs;

    private AssertionProperties(Builder builder) {
        this.mode = builder.mode;
        this.outputCount = builder.outputCount;
        this.timeoutMs = builder.timeoutMs;
    }

    public String getMode() {
        return mode;
    }

    public Integer getOutputCount() {
        return outputCount;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String mode;
        private Integer outputCount;
        private Long timeoutMs;

        public Builder() {
            // Set default values if needed
            this.mode = "negative";
            this.outputCount = 0;
            this.timeoutMs = 10000L;
        }

        public Builder mode(String mode) {
            this.mode = mode;
            return this;
        }

        public Builder outputCount(Integer outputCount) {
            this.outputCount = outputCount;
            return this;
        }

        public Builder timeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public AssertionProperties build() {
            return new AssertionProperties(this);
        }
    }
}