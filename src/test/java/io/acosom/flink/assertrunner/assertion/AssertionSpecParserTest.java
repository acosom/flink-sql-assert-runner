package io.acosom.flink.assertrunner.assertion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AssertionSpecParserTest {

    @Test
    void returnsBuilderDefaultsWhenNoPropertiesPresent() {
        AssertionProperties props = AssertionSpecParser.parse("SELECT * FROM t");

        assertThat(props.getMode()).isEqualTo("negative");
        assertThat(props.getOutputCount()).isZero();
        assertThat(props.getTimeoutMs()).isEqualTo(10_000L);
    }

    @Test
    void parsesAllThreeProperties() {
        String script = "-- mode:positive outputCount:3 timeoutMs:5000\nSELECT * FROM t";

        AssertionProperties props = AssertionSpecParser.parse(script);

        assertThat(props.getMode()).isEqualTo("positive");
        assertThat(props.getOutputCount()).isEqualTo(3);
        assertThat(props.getTimeoutMs()).isEqualTo(5_000L);
    }

    @Test
    void parsesPropertiesAcrossMultipleLines() {
        String script = "-- mode:positive\n-- outputCount:7\nSELECT 1";

        AssertionProperties props = AssertionSpecParser.parse(script);

        assertThat(props.getMode()).isEqualTo("positive");
        assertThat(props.getOutputCount()).isEqualTo(7);
    }

    @Test
    void unknownPropertyIsSkippedNotThrown() {
        String script = "-- mode:positive somethingElse:foo outputCount:2";

        AssertionProperties props = AssertionSpecParser.parse(script);

        assertThat(props.getMode()).isEqualTo("positive");
        assertThat(props.getOutputCount()).isEqualTo(2);
    }

    @Test
    void throwsRuntimeExceptionForNonNumericOutputCount() {
        String script = "-- outputCount:notANumber";

        assertThatThrownBy(() -> AssertionSpecParser.parse(script))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("outputCount");
    }

    @Test
    void throwsRuntimeExceptionForNonNumericTimeout() {
        String script = "-- timeoutMs:slow";

        assertThatThrownBy(() -> AssertionSpecParser.parse(script))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("timeoutMs");
    }

    @Test
    void requiresWhitespaceBeforePropertyToMatch() {
        String script = "WHEREmode:positive";

        AssertionProperties props = AssertionSpecParser.parse(script);

        assertThat(props.getMode()).isEqualTo("negative");
    }

    @Test
    void lastValueWinsWhenSamePropertyAppearsMultipleTimes() {
        String script = "-- mode:positive\n-- mode:negative";

        AssertionProperties props = AssertionSpecParser.parse(script);

        assertThat(props.getMode()).isEqualTo("negative");
    }
}
