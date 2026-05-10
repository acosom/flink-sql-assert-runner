package io.acosom.flink.assertrunner.flink;

import io.acosom.flink.assertrunner.error.FlinkJobException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobCancellationHeaders;
import org.apache.flink.runtime.rest.messages.JobCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobStatusInfoHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarListHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarListInfo;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunResponseBody;
import org.apache.flink.util.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Lifecycle of a Flink job submitted via the JobManager REST API: start, wait
 * for RUNNING, list, cancel.
 */
public final class JobController implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JobController.class);

    private static final int START_MAX_RETRIES = 5;
    private static final long START_RETRY_DELAY_MS = 5_000L;

    private final RestClient restClient;
    private final URI jobmanagerUri;
    private final String entrypointClass;
    private final String sqlFileInJar;

    public JobController(String jobmanagerUrl, String entrypointClass, String sqlFileInJar) {
        try {
            this.restClient = new RestClient(new Configuration(), Executors.newSingleThreadExecutor());
        } catch (ConfigurationException e) {
            throw new FlinkJobException("Failed to create Flink REST client", e);
        }
        this.jobmanagerUri = URI.create(jobmanagerUrl);
        this.entrypointClass = entrypointClass;
        this.sqlFileInJar = sqlFileInJar;
    }

    public void startJob() {
        LOG.info("Starting Flink job using entrypoint {}", entrypointClass);
        String jarId = getJarId();
        JobID jobId = runJobFromJar(jarId);
        waitForRunning(jobId);
    }

    public void cancelAllRunningJobs() {
        LOG.info("Cancelling all running Flink jobs");
        JobIdsWithStatusOverview jobs = listJobs();
        List<JobIdsWithStatusOverview.JobIdWithStatus> running = jobs.getJobsWithStatus().stream()
                .filter(j -> JobStatus.RUNNING.equals(j.getJobStatus()))
                .collect(Collectors.toList());
        running.forEach(this::cancel);
    }

    @Override
    public void close() {
        try {
            restClient.shutdown(Duration.ofSeconds(5));
        } catch (Exception e) {
            LOG.warn("Failed to shut down Flink REST client: {}", e.getMessage());
        }
    }

    private void cancel(JobIdsWithStatusOverview.JobIdWithStatus job) {
        JobCancellationMessageParameters params = new JobCancellationMessageParameters()
                .resolveJobId(job.getJobId())
                .resolveTerminationMode(TerminationModeQueryParameter.TerminationMode.CANCEL);
        try {
            restClient.sendRequest(jobmanagerUri.getHost(), jobmanagerUri.getPort(),
                    JobCancellationHeaders.getInstance(), params,
                    EmptyRequestBody.getInstance()).get();
        } catch (Exception e) {
            throw new FlinkJobException("Failed to cancel job " + job.getJobId(), e);
        }
    }

    private JobIdsWithStatusOverview listJobs() {
        try {
            return restClient.sendRequest(jobmanagerUri.getHost(), jobmanagerUri.getPort(),
                    JobIdsWithStatusesOverviewHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance()).get();
        } catch (Exception e) {
            throw new FlinkJobException("Failed to list Flink jobs", e);
        }
    }

    private String getJarId() {
        JarListInfo response;
        try {
            response = restClient.sendRequest(jobmanagerUri.getHost(), jobmanagerUri.getPort(),
                    JarListHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance()).get();
        } catch (Exception e) {
            throw new FlinkJobException("Failed to list uploaded JARs", e);
        }
        if (response.jarFileList.isEmpty()) {
            throw new FlinkJobException("No JARs uploaded to JobManager");
        }
        return response.jarFileList.get(0).id;
    }

    private JobID runJobFromJar(String jarId) {
        JarRunRequestBody body = new JarRunRequestBody(
                entrypointClass,
                List.of("/opt/flink/sql/" + sqlFileInJar),
                null,  // parallelism
                null,  // jobId
                null,  // allowNonRestoredState
                null,  // savepointPath
                null,  // deprecatedRecoveryClaimMode
                null,  // recoveryClaimMode
                null); // flinkConfiguration

        JarRunMessageParameters params = JarRunHeaders.getInstance().getUnresolvedMessageParameters();
        params.jarIdPathParameter.resolve(jarId);

        JarRunResponseBody response;
        try {
            response = restClient.sendRequest(jobmanagerUri.getHost(), jobmanagerUri.getPort(),
                    JarRunHeaders.getInstance(), params, body).get();
        } catch (Exception e) {
            throw new FlinkJobException("Failed to start job from JAR " + jarId, e);
        }
        return response.getJobId();
    }

    private void waitForRunning(JobID jobId) {
        var statusParams = JobStatusInfoHeaders.getInstance().getUnresolvedMessageParameters();
        statusParams.jobPathParameter.resolve(jobId);

        for (int attempt = 0; attempt < START_MAX_RETRIES; attempt++) {
            try {
                var status = restClient.sendRequest(jobmanagerUri.getHost(), jobmanagerUri.getPort(),
                        JobStatusInfoHeaders.getInstance(), statusParams,
                        EmptyRequestBody.getInstance()).get();
                if (JobStatus.RUNNING.equals(status.getJobStatus())) {
                    LOG.info("Job {} is running", jobId);
                    return;
                }
                Thread.sleep(START_RETRY_DELAY_MS);
            } catch (Exception e) {
                LOG.warn("Failed to verify status of job {}: {}", jobId, e.getMessage());
            }
        }
        throw new FlinkJobException("Job " + jobId + " did not reach RUNNING state");
    }
}
