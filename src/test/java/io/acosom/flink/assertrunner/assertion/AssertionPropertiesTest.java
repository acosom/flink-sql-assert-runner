package io.acosom.flink.assertrunner.assertion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AssertionPropertiesTest {

    @Test
    void builderHasNegativeModeAsDefault() {
        AssertionProperties props = AssertionProperties.builder().build();

        assertThat(props.getMode()).isEqualTo("negative");
    }

    @Test
    void builderHasZeroOutputCountAsDefault() {
        AssertionProperties props = AssertionProperties.builder().build();

        assertThat(props.getOutputCount()).isZero();
    }

    @Test
    void builderHasTenSecondTimeoutAsDefault() {
        AssertionProperties props = AssertionProperties.builder().build();

        assertThat(props.getTimeoutMs()).isEqualTo(10_000L);
    }

    @Test
    void fluentSettersOverrideDefaults() {
        AssertionProperties props = AssertionProperties.builder()
                .mode("positive")
                .outputCount(5)
                .timeoutMs(2_500L)
                .build();

        assertThat(props.getMode()).isEqualTo("positive");
        assertThat(props.getOutputCount()).isEqualTo(5);
        assertThat(props.getTimeoutMs()).isEqualTo(2_500L);
    }

    @Test
    void buildersAreIndependentInstances() {
        AssertionProperties.Builder a = AssertionProperties.builder().mode("positive");
        AssertionProperties.Builder b = AssertionProperties.builder();

        assertThat(a.build().getMode()).isEqualTo("positive");
        assertThat(b.build().getMode()).isEqualTo("negative");
    }
}
