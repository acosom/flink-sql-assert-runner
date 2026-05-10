package io.acosom.flink.assertrunner.unit.compile;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

public class JavaCompilerUtil {
    public static Class<?> compile(File javaFile) {
        try {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
            InMemoryFileManager manager = new InMemoryFileManager(compiler.getStandardFileManager(null, null, null));

            String className = javaFile.getName().split("\\.")[0];
            List<JavaFileObject> sourceFiles = Collections.singletonList(
                    new JavaSourceFromString(className, FileUtils.readFileUtf8(javaFile)));

            JavaCompiler.CompilationTask task = compiler.getTask(null, manager, diagnostics, null, null, sourceFiles);

            boolean result = task.call();

            if (!result) {
                System.out.println("Failed to compile file " + javaFile.getPath());
                System.out.println(diagnostics.getDiagnostics());
                diagnostics.getDiagnostics()
                        .forEach(System.out::println);
            } else {
                ClassLoader classLoader = manager.getClassLoader(null);
                return classLoader.loadClass(className);
            }
        } catch (Exception e) {
            System.out.println("Failed to compile file " + javaFile.getPath() + "\nCause: " + e.getCause());
            e.printStackTrace();
        }
        return null;
    }
}
