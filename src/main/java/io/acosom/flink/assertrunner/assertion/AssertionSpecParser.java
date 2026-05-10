package io.acosom.flink.assertrunner.assertion;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.acosom.flink.assertrunner.assertion.AssertionPropertyEnum.MODE;
import static io.acosom.flink.assertrunner.assertion.AssertionPropertyEnum.OUTPUT_COUNT;
import static io.acosom.flink.assertrunner.assertion.AssertionPropertyEnum.TIMEOUT_MS;

public final class AssertionSpecParser {

    private static final Pattern PARAMETER_PATTERN = Pattern.compile("(?<=\\s)(\\w+):(\\S*)");

    private AssertionSpecParser() {
    }

    public static AssertionProperties parse(String sqlScript) {
        final Matcher matcher = PARAMETER_PATTERN.matcher(sqlScript);

        final AssertionProperties.Builder builder = AssertionProperties.builder();
        while (matcher.find()) {
            String propertyName = matcher.group(1);
            String propertyValue = matcher.group(2);
            if (MODE.getName().equals(propertyName)) {
                builder.mode(propertyValue);
                continue;
            }

            try {
                if (OUTPUT_COUNT.getName().equals(propertyName)) {
                    builder.outputCount(Integer.parseInt(propertyValue));
                    continue;
                }

                if (TIMEOUT_MS.getName().equals(propertyName)) {
                    builder.timeoutMs(Long.parseLong(propertyValue));
                    continue;
                }
            } catch (NumberFormatException e) {
                String errorMessage = String.format(
                        "Failed to parse assertion property value %s for property name %s. Cause %s",
                        propertyValue, propertyName, e.getMessage());
                System.out.println(errorMessage);
                throw new RuntimeException(errorMessage);
            }
            System.out.println("Unknown assertion property: " + propertyName);
        }
        return builder.build();
    }
}
