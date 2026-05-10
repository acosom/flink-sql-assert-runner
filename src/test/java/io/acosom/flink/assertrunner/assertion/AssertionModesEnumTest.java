package io.acosom.flink.assertrunner.assertion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AssertionModesEnumTest {

    @Test
    void positiveModeName() {
        assertThat(AssertionModesEnum.POSITIVE.getName()).isEqualTo("positive");
    }

    @Test
    void negativeModeName() {
        assertThat(AssertionModesEnum.NEGATIVE.getName()).isEqualTo("negative");
    }

    @Test
    void exactlyTwoModesExist() {
        assertThat(AssertionModesEnum.values()).hasSize(2);
    }
}
