package io.acosom.flink.assertrunner.template;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EnvVarTemplatingTest {

    @Test
    void replacesSinglePlaceholderFromEnv() {
        String result = EnvVarTemplating.apply("topic = '@@TOPIC@@'", Map.of("TOPIC", "events"));

        assertThat(result).isEqualTo("topic = 'events'");
    }

    @Test
    void replacesMultiplePlaceholders() {
        String result = EnvVarTemplating.apply(
                "@@A@@-@@B@@", Map.of("A", "left", "B", "right"));

        assertThat(result).isEqualTo("left-right");
    }

    @Test
    void leavesUnknownPlaceholderUnchanged() {
        String result = EnvVarTemplating.apply("k=@@MISSING@@", Map.of());

        assertThat(result).isEqualTo("k=@@MISSING@@");
    }

    @Test
    void escapesSingleQuotesInValue() {
        String result = EnvVarTemplating.apply(
                "name = '@@USER@@'", Map.of("USER", "O'Brien"));

        assertThat(result).isEqualTo("name = 'O''Brien'");
    }

    @Test
    void allowsUnderscoresAndHyphensInPlaceholderNames() {
        String result = EnvVarTemplating.apply(
                "@@FOO_BAR@@ @@A-B@@", Map.of("FOO_BAR", "x", "A-B", "y"));

        assertThat(result).isEqualTo("x y");
    }

    @Test
    void doesNotMatchPlaceholdersWithDigitsOnly() {
        String result = EnvVarTemplating.apply("@@123@@", Map.of("123", "x"));

        assertThat(result).isEqualTo("@@123@@");
    }

    @Test
    void returnsInputUnchangedWhenNoPlaceholdersPresent() {
        String result = EnvVarTemplating.apply("plain text", Map.of("X", "y"));

        assertThat(result).isEqualTo("plain text");
    }

    @Test
    void replacementValueWithBackslashIsNotInterpretedAsRegexEscape() {
        String result = EnvVarTemplating.apply(
                "@@X@@", Map.of("X", "C:\\path\\to\\$file"));

        assertThat(result).isEqualTo("C:\\path\\to\\$file");
    }
}
