package io.acosom.flink.assertrunner.e2e;

import io.acosom.flink.assertrunner.flink.JobController;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * End-to-end test for {@link JobController} against a real Flink session
 * cluster booted via Testcontainers. Validates the REST plumbing
 * ({@code JarRunRequestBody}, jar list, status polling, cancel) — the surface
 * that breaks between Flink majors.
 *
 * <p>The Flink image version follows the build's {@code ${flink.version}}
 * property (1.20.4 on {@code main}, 2.0.1 on {@code flink-2.0}). The test JAR
 * is built at runtime from the compiled {@link MinimalSqlRunner} class so it
 * stays in lockstep with the project's Flink version.</p>
 */
@Tag("integration")
class FlinkJobControllerIT {

    private static final String FLINK_IMAGE = "flink:" + System.getProperty("flink.version", "1.20.4")
            + "-scala_2.12-java17";

    private static final Network NETWORK = Network.newNetwork();

    private static final GenericContainer<?> JOBMANAGER = new GenericContainer<>(
            DockerImageName.parse(FLINK_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("jobmanager")
            .withExposedPorts(8081)
            .withCommand("jobmanager")
            .withEnv("FLINK_PROPERTIES",
                    "jobmanager.rpc.address: jobmanager\n"
                            + "rest.bind-address: 0.0.0.0\n"
                            + "rest.address: 0.0.0.0\n")
            .waitingFor(Wait.forHttp("/config").forPort(8081).withStartupTimeout(Duration.ofMinutes(2)));

    private static final GenericContainer<?> TASKMANAGER = new GenericContainer<>(
            DockerImageName.parse(FLINK_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("taskmanager")
            .dependsOn(JOBMANAGER)
            .withCommand("taskmanager")
            .withEnv("FLINK_PROPERTIES",
                    "jobmanager.rpc.address: jobmanager\n"
                            + "taskmanager.numberOfTaskSlots: 2\n");

    private static String jobmanagerUrl;

    @BeforeAll
    static void startCluster(@org.junit.jupiter.api.io.TempDir Path tempDir) throws Exception {
        JOBMANAGER.start();
        TASKMANAGER.start();
        jobmanagerUrl = "http://" + JOBMANAGER.getHost() + ":" + JOBMANAGER.getMappedPort(8081);

        waitForTaskSlot(Duration.ofMinutes(1));

        Path jar = packMinimalRunnerJar(tempDir);
        uploadJar(jar);
    }

    @AfterAll
    static void stopCluster() {
        TASKMANAGER.stop();
        JOBMANAGER.stop();
        NETWORK.close();
    }

    @Test
    void startsJobAndReachesRunningState() {
        try (JobController controller = new JobController(jobmanagerUrl,
                MinimalSqlRunner.class.getName(), "ignored.sql")) {
            assertDoesNotThrow(controller::startJob);
            controller.cancelAllRunningJobs();
        }
    }

    @Test
    void cancelAllRunningJobsIsNoOpWhenNoneRunning() {
        try (JobController controller = new JobController(jobmanagerUrl,
                MinimalSqlRunner.class.getName(), "ignored.sql")) {
            assertDoesNotThrow(controller::cancelAllRunningJobs);
        }
    }

    @Test
    void closeIsIdempotent() {
        JobController controller = new JobController(jobmanagerUrl,
                MinimalSqlRunner.class.getName(), "ignored.sql");
        controller.close();
        assertDoesNotThrow(controller::close);
    }

    private static void waitForTaskSlot(Duration timeout) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder()
                            .uri(URI.create(jobmanagerUrl + "/overview"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200 && resp.body().contains("\"slots-available\":") &&
                    !resp.body().contains("\"slots-available\":0")) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("TaskManager did not register a slot in time");
    }

    private static Path packMinimalRunnerJar(Path tempDir) throws IOException {
        Path jar = tempDir.resolve("minimal-sql-runner-" + UUID.randomUUID() + ".jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            String classResource = MinimalSqlRunner.class.getName().replace('.', '/') + ".class";
            try (InputStream in = MinimalSqlRunner.class.getClassLoader().getResourceAsStream(classResource)) {
                if (in == null) {
                    throw new IllegalStateException("Could not find compiled MinimalSqlRunner class");
                }
                jos.putNextEntry(new JarEntry(classResource));
                in.transferTo(jos);
                jos.closeEntry();
            }
        }
        return jar;
    }

    private static void uploadJar(Path jar) throws Exception {
        byte[] body = buildMultipartBody(jar);
        HttpResponse<String> resp = HttpClient.newHttpClient().send(HttpRequest.newBuilder()
                        .uri(URI.create(jobmanagerUrl + "/jars/upload"))
                        .header("Content-Type", "multipart/form-data; boundary=" + BOUNDARY)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode())
                .as("jar upload response: %s", resp.body())
                .isEqualTo(200);
    }

    private static final String BOUNDARY = "----flinkSqlAssertRunnerBoundary" + UUID.randomUUID();

    private static byte[] buildMultipartBody(Path jar) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String preamble = "--" + BOUNDARY + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jar.getFileName() + "\"\r\n"
                + "Content-Type: application/x-java-archive\r\n\r\n";
        out.write(preamble.getBytes(StandardCharsets.UTF_8));
        out.write(Files.readAllBytes(jar));
        out.write(("\r\n--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return out.toByteArray();
    }
}
