package io.acosom.flink.assertrunner.assertion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AssertionPropertyEnumTest {

    @Test
    void modePropertyName() {
        assertThat(AssertionPropertyEnum.MODE.getName()).isEqualTo("mode");
    }

    @Test
    void outputCountPropertyName() {
        assertThat(AssertionPropertyEnum.OUTPUT_COUNT.getName()).isEqualTo("outputCount");
    }

    @Test
    void timeoutMsPropertyName() {
        assertThat(AssertionPropertyEnum.TIMEOUT_MS.getName()).isEqualTo("timeoutMs");
    }

    @Test
    void exactlyThreePropertiesExist() {
        assertThat(AssertionPropertyEnum.values()).hasSize(3);
    }
}
