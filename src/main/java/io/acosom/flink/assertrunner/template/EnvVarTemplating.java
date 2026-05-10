package io.acosom.flink.assertrunner.template;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EnvVarTemplating {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("@@([a-zA-Z_-]+)@@");

    private EnvVarTemplating() {
    }

    public static String apply(String input, Map<String, String> env) {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(input);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String variable = matcher.group(1);
            if (!env.containsKey(variable)) {
                System.out.println("Did not replace variable '" + variable + "' because it was not set.");
                continue;
            }

            String value = env.get(variable);
            String escapedValue = escapeVariable(value);
            String quotedValue = Matcher.quoteReplacement(escapedValue);
            matcher.appendReplacement(result, quotedValue);
        }

        matcher.appendTail(result);
        return result.toString();
    }

    private static String escapeVariable(String variable) {
        return variable.replace("'", "''");
    }
}
