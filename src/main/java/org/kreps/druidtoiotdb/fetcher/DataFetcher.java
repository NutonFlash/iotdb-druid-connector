package org.kreps.druidtoiotdb.fetcher;

import org.kreps.druidtoiotdb.config.AppConfig;
import org.kreps.druidtoiotdb.model.DataPoint;
import org.kreps.druidtoiotdb.model.FailedRequest;
import org.kreps.druidtoiotdb.utils.FailedRequestLogger;
import org.kreps.druidtoiotdb.utils.RetryUtils;
import org.kreps.druidtoiotdb.worker.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kreps.druidtoiotdb.exceptions.ServerErrorException;
import org.kreps.druidtoiotdb.exceptions.ClientErrorException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DataFetcher implements Runnable, AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(DataFetcher.class);
    private final String logPrefix;

    private final AppConfig config;
    private final BlockingQueue<DataPoint> dataQueue;
    private final List<String> assignedTags;
    private volatile boolean running = true;
    private static final DateTimeFormatter API_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String PWCM_CD = "ST";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final CountDownLatch fetcherCompletionLatch;
    private final WorkerManager workerManager;
    private volatile Thread fetcherThread;

    public DataFetcher(AppConfig config, BlockingQueue<DataPoint> dataQueue, List<String> assignedTags,
            int fetcherId, CountDownLatch fetcherCompletionLatch, WorkerManager workerManager) {
        this.config = config;
        this.dataQueue = dataQueue;
        this.assignedTags = assignedTags;
        this.logPrefix = String.format("Fetcher-%d", fetcherId);
        this.fetcherCompletionLatch = fetcherCompletionLatch;
        this.workerManager = workerManager;
    }

    @Override
    public void run() {
        fetcherThread = Thread.currentThread();
        try {
            logger.info("{} started with {} tags", logPrefix, assignedTags.size());
            processAssignedTags();
        } catch (Exception e) {
            logger.error("{} encountered error: ", logPrefix, e);
        } finally {
            fetcherCompletionLatch.countDown();
            logger.info("{} stopped", logPrefix);
        }
    }

    private void processAssignedTags() {
        LocalDateTime globalStart = config.getSourceConfig().getTimeRange().getStartTime();
        LocalDateTime globalEnd = config.getSourceConfig().getTimeRange().getEndTime();
        int batchSize = config.getProcessingConfig().getBatch().getReadSize();

        List<TimeInterval> intervals = calculateTimeIntervals(globalStart, globalEnd, batchSize);
        logger.info("{} created {} time intervals", logPrefix, intervals.size());

        for (String tag : assignedTags) {
            if (!running) {
                return;
            }

            processTagIntervals(tag, intervals);
        }
        logger.info("{} completed processing all assigned tags", logPrefix);
    }

    private void processTagIntervals(String tag, List<TimeInterval> intervals) {
        for (TimeInterval interval : intervals) {
            if (!running) {
                return;
            }
            try {
                processDataPoints(tag, interval.start, interval.end);
            } catch (Exception e) {
                handleFetchError(e, tag, interval.start, interval.end);
            }
        }
    }

    private List<Map<String, String>> fetchDataWithRetry(String tag, LocalDateTime start, LocalDateTime end)
            throws Exception {
        return RetryUtils.executeWithRetry(() -> {
            URIBuilder builder = buildApiRequest(tag, start, end);
            return executeHttpRequest(builder);
        }, config.getRetryConfig(), String.format("Fetch data for tag %s", tag));
    }

    private void queueDataPointsWithBackpressure(List<Map<String, String>> dataPoints,
            String tag, LocalDateTime start, LocalDateTime end) throws InterruptedException {
        for (Map<String, String> point : dataPoints) {
            while (running) {
                if (dataQueue.offer(new DataPoint(point), 30, TimeUnit.SECONDS)) {
                    break;
                }
                logger.warn("{} queue is full (size: {}), waiting before retry. Tag: {}, Interval: [{} - {}]",
                        logPrefix, dataQueue.size(), tag, start, end);
                Thread.sleep(5000);
            }
            if (!running) {
                throw new InterruptedException("Fetcher stopped while queueing data");
            }
        }
    }

    private void processDataPoints(String tag, LocalDateTime start, LocalDateTime end) throws Exception {
        List<Map<String, String>> dataPoints = fetchDataWithRetry(tag, start, end);
        if (dataPoints.isEmpty()) {
            return;
        }

        queueDataPointsWithBackpressure(dataPoints, tag, start, end);
        logger.info("{} processed {} points for tag {} in interval [{} - {}]",
                logPrefix, dataPoints.size(), tag, start, end);
    }

    private void handleFetchError(Exception e, String tag, LocalDateTime start, LocalDateTime end) {
        if (e instanceof ClientErrorException) {
            logClientError(tag, start, end, (ClientErrorException) e);
        } else if (e instanceof ServerErrorException) {
            handleServerError(tag, start, end, (ServerErrorException) e);
        }
    }

    private void logClientError(String tag, LocalDateTime start, LocalDateTime end, ClientErrorException e) {
        logger.error("{} Client error - skipping tag {}: {}", logPrefix, tag, e.getMessage());
        FailedRequestLogger.logFailedRequest(
                new FailedRequest(tag, start, end, e.getMessage(), e.getStatusCode(), true));
    }

    private void handleServerError(String tag, LocalDateTime start, LocalDateTime end, ServerErrorException e) {
        logger.error("{} Server error - all retries failed for tag {}: {}", logPrefix, tag, e.getMessage());
        FailedRequestLogger.logFailedRequest(
                new FailedRequest(tag, start, end, e.getMessage(), e.getStatusCode()));
        workerManager.initiateShutdown();
        logger.error("{} Maximum retry attempts reached, initiated shutdown.", logPrefix);
    }

    private URIBuilder buildApiRequest(String tag, LocalDateTime start, LocalDateTime end) throws Exception {
        String apiUrl = config.getSourceConfig().getDruidSettings().getApiUrl();
        String userKey = config.getSourceConfig().getDruidSettings().getUserKey();

        return new URIBuilder(apiUrl)
                .addParameter("stime", start.format(API_DATE_FORMAT))
                .addParameter("etime", end.format(API_DATE_FORMAT))
                .addParameter("tags", tag)
                .addParameter("PWCM_CD", PWCM_CD)
                .addParameter("USER_KEY", userKey);
    }

    private List<Map<String, String>> executeHttpRequest(URIBuilder builder) throws Exception {
        HttpGet request = new HttpGet(builder.build());
        String requestUrl = java.net.URLDecoder.decode(request.getURI().toString(), "UTF-8");

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            handleHttpStatusCode(statusCode, response, requestUrl);

            String jsonResponse = EntityUtils.toString(response.getEntity());
            return objectMapper.readValue(jsonResponse, new TypeReference<List<Map<String, String>>>() {
            });
        }
    }

    private void handleHttpStatusCode(int statusCode, CloseableHttpResponse response, String requestUrl)
            throws ServerErrorException {
        if (statusCode >= 400 && statusCode < 500) {
            String message = String.format("Client error: HTTP %d: %s. Request URL: %s",
                    statusCode, response.getStatusLine().getReasonPhrase(), requestUrl);
            throw new ClientErrorException(message, statusCode);
        }
        if (statusCode >= 500) {
            throw new ServerErrorException("HTTP " + statusCode + ": " +
                    response.getStatusLine().getReasonPhrase(), statusCode);
        }
    }

    private List<TimeInterval> calculateTimeIntervals(LocalDateTime start, LocalDateTime end, int batchSize) {
        List<TimeInterval> intervals = new ArrayList<>();
        long intervalSeconds = batchSize;

        LocalDateTime currentStart = start;
        while (currentStart.isBefore(end)) {
            LocalDateTime currentEnd = currentStart.plusSeconds(intervalSeconds);
            if (currentEnd.isAfter(end)) {
                currentEnd = end;
            }

            intervals.add(new TimeInterval(currentStart, currentEnd));
            currentStart = currentEnd;
        }

        return intervals;
    }

    private static class TimeInterval {
        final LocalDateTime start;
        final LocalDateTime end;

        TimeInterval(LocalDateTime start, LocalDateTime end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return String.format("[%s - %s]", start, end);
        }
    }

    public void stop() {
        running = false;
        Thread thread = fetcherThread;
        if (thread != null) {
            thread.interrupt();
        }
        fetcherCompletionLatch.countDown();
        logger.info("{} stopped", logPrefix);
    }

    @Override
    public void close() {
        try {
            stop();
            httpClient.close();
        } catch (IOException e) {
            logger.error("{} Error closing HTTP client: ", logPrefix, e);
        }
    }
}
