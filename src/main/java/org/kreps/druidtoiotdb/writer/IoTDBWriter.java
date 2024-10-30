package org.kreps.druidtoiotdb.writer;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.kreps.druidtoiotdb.config.AppConfig;
import org.kreps.druidtoiotdb.iotdb.IoTDBSessionPool;
import org.kreps.druidtoiotdb.model.DataPoint;
import org.kreps.druidtoiotdb.model.FailedWrite;
import org.kreps.druidtoiotdb.utils.FailedWriteLogger;
import org.kreps.druidtoiotdb.validator.SchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.kreps.druidtoiotdb.utils.RetryUtils;
import org.kreps.druidtoiotdb.worker.WorkerManager;

public class IoTDBWriter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IoTDBWriter.class);
    private final String logPrefix;

    private final AppConfig config;
    private final BlockingQueue<DataPoint> dataQueue;
    private final IoTDBSessionPool iotdbSessionPool;
    private final SchemaValidator schemaValidator;
    private final CountDownLatch writerCompletionLatch;
    private final WorkerManager workerManager;

    private volatile boolean running = true;
    private volatile Thread writerThread;

    // Measurement schemas for tablet creation
    private static final List<MeasurementSchema> MEASUREMENT_SCHEMAS = Arrays.asList(
            new MeasurementSchema("Qual", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY),
            new MeasurementSchema("ColTime", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY),
            new MeasurementSchema("std_tag", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY),
            new MeasurementSchema("SensorType", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY),
            new MeasurementSchema("Val", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY));

    public IoTDBWriter(AppConfig config, BlockingQueue<DataPoint> dataQueue, IoTDBSessionPool iotdbSessionPool,
            CountDownLatch writerCompletionLatch, WorkerManager workerManager, int writerId) {
        this.config = config;
        this.dataQueue = dataQueue;
        this.iotdbSessionPool = iotdbSessionPool;
        this.schemaValidator = new SchemaValidator(
                iotdbSessionPool.getSessionPool(),
                config.getRetryConfig());
        this.writerCompletionLatch = writerCompletionLatch;
        this.workerManager = workerManager;
        this.logPrefix = String.format("Writer-%d", writerId);
    }

    @Override
    public void run() {
        writerThread = Thread.currentThread();
        try {
            logger.info("{} started", logPrefix);
            processData();
        } finally {
            writerCompletionLatch.countDown();
            logger.info("{} stopped", logPrefix);
        }
    }

    public void stop() {
        running = false;
        Thread thread = writerThread;
        if (thread != null) {
            thread.interrupt();
        }
        writerCompletionLatch.countDown();
        logger.info("{} stopped", logPrefix);
    }

    private void processData() {
        while (running) {
            try {
                List<DataPoint> batch = collectBatch();
                if (batch.isEmpty()) {
                    continue;
                }

                if (batch.stream().anyMatch(DataPoint::isPoisonPill)) {
                    logger.info("{} received poison pill, stopping", logPrefix);
                    dataQueue.put(DataPoint.POISON_PILL);
                    break;
                }

                // Validate schema before processing
                try {
                    schemaValidator.validateDataPoints(batch);
                } catch (Exception e) {
                    logger.error("{} Schema validation failed: {}", logPrefix, e.getMessage());
                    if (e instanceof IoTDBConnectionException) {
                        handleCriticalError(e);
                        break;
                    }
                    // Log failed batch and continue
                    batch.forEach(point -> {
                        String devicePath = point.getTimeseriesPath();
                        logFailedWrite(devicePath, Collections.singletonList(point),
                                "Schema validation failed: " + e.getMessage());
                    });
                    continue;
                }

                Map<String, List<DataPoint>> deviceGroups = groupByDevice(batch);
                Map<String, Tablet> tablets = createTablets(deviceGroups);
                writeTablets(tablets, deviceGroups);
                logger.info("{} Successfully inserted {} tablets with {} total points",
                        logPrefix, tablets.size(), batch.size());

            } catch (InterruptedException e) {
                logger.info("{} interrupted, stopping gracefully", logPrefix);
                break;
            } catch (Exception e) {
                logger.error("{} Error processing batch: {}", logPrefix, e.getMessage());
                if (e.getCause() instanceof IoTDBConnectionException) {
                    handleCriticalError(e);
                    break;
                }
            }
        }
    }

    private List<DataPoint> collectBatch() throws InterruptedException {
        List<DataPoint> batch = new ArrayList<>();
        int batchSize = config.getProcessingConfig().getBatch().getWriteSize();

        DataPoint point = dataQueue.poll(5, TimeUnit.SECONDS);
        if (point == null) {
            return batch;
        }

        batch.add(point);
        while (batch.size() < batchSize && running) {
            point = dataQueue.poll(100, TimeUnit.MILLISECONDS);
            if (point == null)
                break;
            if (point.isPoisonPill()) {
                batch.add(point);
                break;
            }
            batch.add(point);
        }

        return batch;
    }

    private void writeTablets(Map<String, Tablet> tablets, Map<String, List<DataPoint>> deviceGroups) {
        for (Map.Entry<String, Tablet> entry : tablets.entrySet()) {
            String devicePath = entry.getKey();
            Tablet tablet = entry.getValue();
            List<DataPoint> points = deviceGroups.get(devicePath);

            try {
                RetryUtils.executeWithRetry(() -> {
                    if (!iotdbSessionPool.isAvailable()) {
                        throw new IoTDBConnectionException("IoTDB connection is not available");
                    }
                    iotdbSessionPool.getSessionPool().insertTablet(tablet);
                    return null;
                }, config.getRetryConfig(), "Insert tablet for " + devicePath);
            } catch (Exception e) {
                handleWriteError(devicePath, points, e);
                if (e instanceof IoTDBConnectionException ||
                        (e.getCause() != null && e.getCause() instanceof IoTDBConnectionException)) {
                    handleCriticalError(e);
                    return; // Exit the loop on connection issues
                }
            }
        }
    }

    private void handleWriteError(String devicePath, List<DataPoint> points, Exception e) {
        String errorMessage = e.getMessage();
        logFailedWrite(devicePath, points, errorMessage);

        if (e instanceof IoTDBConnectionException) {
            handleCriticalError(e);
        }
    }

    private void handleCriticalError(Exception e) {
        logger.error("{} Critical error encountered: {}. Initiating shutdown...", logPrefix, e.getMessage());
        workerManager.initiateShutdown();
    }

    private void logFailedWrite(String devicePath, List<DataPoint> points, String errorMessage) {
        String tag = devicePath.substring(devicePath.lastIndexOf('.') + 1).replace("`", "");
        FailedWrite failedWrite = new FailedWrite(tag, devicePath, points, errorMessage);
        FailedWriteLogger.logFailedWrite(failedWrite);
    }

    private Map<String, List<DataPoint>> groupByDevice(List<DataPoint> batch) {
        Map<String, List<DataPoint>> deviceGroups = new HashMap<>();

        for (DataPoint point : batch) {
            String devicePath = point.getTimeseriesPath();
            deviceGroups.computeIfAbsent(devicePath, k -> new ArrayList<>()).add(point);
        }

        return deviceGroups;
    }

    private Map<String, Tablet> createTablets(Map<String, List<DataPoint>> deviceGroups) throws Exception {
        Map<String, Tablet> tablets = new HashMap<>();
        List<Exception> errors = new ArrayList<>();

        for (Map.Entry<String, List<DataPoint>> entry : deviceGroups.entrySet()) {
            String devicePath = entry.getKey();
            List<DataPoint> points = entry.getValue();

            try {
                Tablet tablet = createTablet(devicePath, points);
                if (tablet.rowSize > 0) { // Only add if tablet has data
                    tablets.put(devicePath, tablet);
                } else {
                    logger.warn("{} Skipping empty tablet for device {}", logPrefix, devicePath);
                }
            } catch (Exception e) {
                String error = String.format("Failed to create tablet for device %s: %s",
                        devicePath, e.getMessage());
                errors.add(new RuntimeException(error, e));
                logFailedWrite(devicePath, points, e.getMessage());
            }
        }

        if (!errors.isEmpty()) {
            Exception firstError = errors.get(0);
            logger.error("{} Failed to create {} tablets", logPrefix, errors.size());
            throw new RuntimeException("Failed to create tablets", firstError);
        }

        return tablets;
    }

    private Tablet createTablet(String devicePath, List<DataPoint> points) throws Exception {
        Tablet tablet = new Tablet(devicePath, MEASUREMENT_SCHEMAS, points.size());
        List<Exception> errors = new ArrayList<>();

        for (int i = 0; i < points.size(); i++) {
            DataPoint point = points.get(i);
            tablet.addTimestamp(i, point.getTimestamp());

            for (MeasurementSchema schema : MEASUREMENT_SCHEMAS) {
                String measurementName = schema.getMeasurementId();
                Object value = point.getMeasurements().get(measurementName);

                if (value != null) {
                    try {
                        tablet.addValue(measurementName, i, value);
                    } catch (Exception e) {
                        String error = String.format("Failed to add value for measurement %s: %s",
                                measurementName, e.getMessage());
                        errors.add(new RuntimeException(error, e));
                    }
                } else {
                    logger.warn("{} missing value for measurement {} in row {} for device {}",
                            logPrefix, measurementName, i, devicePath);
                }
            }
        }

        if (!errors.isEmpty()) {
            Exception firstError = errors.get(0);
            logger.error("{} Failed to add {} values to tablet", logPrefix, errors.size());
            throw new RuntimeException("Failed to create tablet values", firstError);
        }

        tablet.rowSize = points.size();
        return tablet;
    }
}
