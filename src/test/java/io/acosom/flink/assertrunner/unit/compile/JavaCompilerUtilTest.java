package io.acosom.flink.assertrunner.unit.compile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class JavaCompilerUtilTest {

    @Test
    void compilesValidJavaSourceFile(@TempDir Path tempDir) throws Exception {
        Path source = writeSource(tempDir, "Greeter",
                "public class Greeter { public String hello() { return \"hi\"; } }");

        Class<?> compiled = JavaCompilerUtil.compile(source.toFile());

        assertThat(compiled).isNotNull();
        assertThat(compiled.getSimpleName()).isEqualTo("Greeter");

        Object instance = compiled.getDeclaredConstructor().newInstance();
        Method hello = compiled.getMethod("hello");
        assertThat(hello.invoke(instance)).isEqualTo("hi");
    }

    @Test
    void returnsNullForInvalidJavaSource(@TempDir Path tempDir) throws IOException {
        Path source = writeSource(tempDir, "Broken",
                "public class Broken { this is not valid java }");

        Class<?> compiled = JavaCompilerUtil.compile(source.toFile());

        assertThat(compiled).isNull();
    }

    @Test
    void returnsNullWhenFileDoesNotExist(@TempDir Path tempDir) {
        Class<?> compiled = JavaCompilerUtil.compile(tempDir.resolve("Missing.java").toFile());

        assertThat(compiled).isNull();
    }

    private static Path writeSource(Path dir, String className, String body) throws IOException {
        Path file = dir.resolve(className + ".java");
        Files.writeString(file, body);
        return file;
    }
}
