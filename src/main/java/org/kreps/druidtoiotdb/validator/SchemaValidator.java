package org.kreps.druidtoiotdb.validator;

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.kreps.druidtoiotdb.config.RetryConfig;
import org.kreps.druidtoiotdb.exceptions.IoTDBInitializationException;
import org.kreps.druidtoiotdb.model.DataPoint;
import org.kreps.druidtoiotdb.utils.RetryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Collections;
import java.util.ArrayList;

public class SchemaValidator {
    private static final Logger logger = LoggerFactory.getLogger(SchemaValidator.class);
    private final SessionPool sessionPool;
    private static final String TEMPLATE_NAME = "druid_t";
    private static final String ROOT_DATABASE = "root.cepco";
    private final RetryConfig retryConfig;
    // Cache of validated device paths
    private final Set<String> validatedDevices = Collections.synchronizedSet(new HashSet<>());

    public SchemaValidator(SessionPool sessionPool, RetryConfig retryConfig) {
        this.sessionPool = sessionPool;
        this.retryConfig = retryConfig;
    }

    public void initializeSchema() throws IoTDBInitializationException {
        createTemplateIfNotExists();
        createRootDatabaseIfNotExists();
    }

    private void createTemplateIfNotExists() throws IoTDBInitializationException {
        logger.info("Checking if template exists: {}", TEMPLATE_NAME);
        try {
            List<String> templates = sessionPool.showAllTemplates();
            if (!templates.contains(TEMPLATE_NAME)) {
                createTemplate();
                logger.info("Created template: {}", TEMPLATE_NAME);
            } else {
                logger.info("Template already exists: {}", TEMPLATE_NAME);
            }
        } catch (StatementExecutionException e) {
            throw new IoTDBInitializationException(
                    "Failed to check template existence due to invalid SQL statement or insufficient permissions: "
                            + e.getMessage(),
                    e);
        } catch (IoTDBConnectionException e) {
            throw new IoTDBInitializationException(
                    "Failed to check template existence due to connection issues. Please verify IoTDB server is running and network is stable: "
                            + e.getMessage(),
                    e);
        }
    }

    private void createTemplate() throws IoTDBInitializationException {
        logger.info("Creating template: {}", TEMPLATE_NAME);

        Template template = new Template(TEMPLATE_NAME, true);

        MeasurementNode qualNode = new MeasurementNode(
                "Qual",
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY);

        MeasurementNode colTimeNode = new MeasurementNode(
                "ColTime",
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY);

        MeasurementNode stdTagNode = new MeasurementNode(
                "std_tag",
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY);

        MeasurementNode sensorTypeNode = new MeasurementNode(
                "SensorType",
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY);

        MeasurementNode valNode = new MeasurementNode(
                "Val",
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY);

        try {
            template.addToTemplate(qualNode);
            template.addToTemplate(colTimeNode);
            template.addToTemplate(stdTagNode);
            template.addToTemplate(sensorTypeNode);
            template.addToTemplate(valNode);

            sessionPool.createSchemaTemplate(template);
            logger.info("Successfully created template: {}", TEMPLATE_NAME);
        } catch (StatementExecutionException e) {
            throw new IoTDBInitializationException(
                    "Failed to create template due to invalid SQL statement or insufficient permissions: "
                            + e.getMessage(),
                    e);
        } catch (IoTDBConnectionException e) {
            throw new IoTDBInitializationException(
                    "Failed to create template - connection error. Please check if IoTDB server is running and accessible",
                    e);
        } catch (IOException e) {
            throw new IoTDBInitializationException(
                    "Failed to create template due to I/O error. Please check system resources and permissions", e);
        }
    }

    private void createRootDatabaseIfNotExists() throws IoTDBInitializationException {
        logger.info("Ensuring root database exists: {}", ROOT_DATABASE);
        try {
            sessionPool.createDatabase(ROOT_DATABASE);
            logger.info("Successfully created database: {}", ROOT_DATABASE);
        } catch (StatementExecutionException e) {
            if (e.getMessage().contains(ROOT_DATABASE)) {
                logger.info("Database already exists: {}", ROOT_DATABASE);
            } else {
                throw new IoTDBInitializationException("Failed to create database due to unexpected error", e);
            }
        } catch (IoTDBConnectionException e) {
            throw new IoTDBInitializationException(
                    "Failed to create database - connection error. Please check if IoTDB server is running and accessible",
                    e);
        }
    }

    public void validateDataPoints(List<DataPoint> dataPoints) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            Set<String> devicePaths = dataPoints.stream()
                    .map(DataPoint::getTimeseriesPath)
                    .filter(path -> !validatedDevices.contains(path))
                    .collect(Collectors.toSet());

            if (devicePaths.isEmpty()) {
                return;
            }

            logger.debug("Validating schema for {} new devices", devicePaths.size());

            for (List<String> batch : createBatches(devicePaths, 100)) {
                try {
                    RetryUtils.executeWithRetry(() -> {
                        for (String path : batch) {
                            try {
                                sessionPool.setSchemaTemplate(TEMPLATE_NAME, path);
                                validatedDevices.add(path);
                            } catch (StatementExecutionException e) {
                                if (e.getMessage().contains("already exists")) {
                                    validatedDevices.add(path);
                                } else {
                                    throw e;
                                }
                            }
                        }
                        return null;
                    }, retryConfig, "Schema validation");
                } catch (Exception e) {
                    String errorMessage = e.getMessage();
                    if (e.getCause() != null) {
                        errorMessage = e.getCause().getMessage();
                    }
                    logger.error("Schema validation failed after {}ms: {}",
                            System.currentTimeMillis() - startTime,
                            errorMessage.split("\n")[0]); // Take only first line of error
                    throw e;
                }
            }

            logger.debug("Schema validation completed in {}ms for {} devices",
                    System.currentTimeMillis() - startTime, devicePaths.size());

        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (e.getCause() != null) {
                errorMessage = e.getCause().getMessage();
            }
            logger.error("Schema validation failed after {}ms: {}",
                    System.currentTimeMillis() - startTime,
                    errorMessage.split("\n")[0]); // Take only first line of error
            throw e;
        }
    }

    private List<List<String>> createBatches(Set<String> items, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        List<String> currentBatch = new ArrayList<>();

        for (String item : items) {
            currentBatch.add(item);
            if (currentBatch.size() >= batchSize) {
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
            }
        }

        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        return batches;
    }
}
