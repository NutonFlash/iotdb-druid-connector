package org.kreps.druidtoiotdb;

import org.kreps.druidtoiotdb.config.AppConfig;
import org.kreps.druidtoiotdb.config.ConfigLoader;
import org.kreps.druidtoiotdb.config.ConfigValidationException;
import org.kreps.druidtoiotdb.iotdb.IoTDBSessionPool;
import org.kreps.druidtoiotdb.model.DataPoint;
import org.kreps.druidtoiotdb.validator.SchemaValidator;
import org.kreps.druidtoiotdb.threading.ThreadPoolManager;
import org.kreps.druidtoiotdb.worker.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final BlockingQueue<DataPoint> dataQueue;
    private final IoTDBSessionPool iotdbSessionPool;
    private final ThreadPoolManager threadPoolManager;
    private final WorkerManager workerManager;
    private final AppConfig config;

    public Main(AppConfig config) {
        this.dataQueue = new LinkedBlockingQueue<>(config.getProcessingConfig().getQueueSize());
        this.iotdbSessionPool = new IoTDBSessionPool(config);

        int readerPoolSize = config.getProcessingConfig().getThreads().getReaderPoolSize();
        int writerPoolSize = config.getProcessingConfig().getThreads().getWriterPoolSize();

        this.threadPoolManager = new ThreadPoolManager(readerPoolSize, writerPoolSize);
        this.workerManager = new WorkerManager(
                config,
                dataQueue,
                threadPoolManager,
                iotdbSessionPool);
        this.config = config;
    }

    public static void main(String[] args) {
        try {
            AppConfig config = ConfigLoader.loadConfig();
            Main app = new Main(config);
            app.run();
        } catch (ConfigValidationException e) {
            logger.error("Configuration error: {}", e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Application failed: ", e);
            System.exit(1);
        }
    }

    private void run() throws Exception {
        try {
            validateSchema();
            workerManager.startWorkers();
            threadPoolManager.waitForFetchers();
            workerManager.sendPoisonPills();
            threadPoolManager.waitForWriters();
        } finally {
            cleanup();
        }
    }

    private void validateSchema() throws Exception {
        logger.info("Validating IoTDB schema...");
        SchemaValidator validator = new SchemaValidator(
            iotdbSessionPool.getSessionPool(),
            config.getRetryConfig()
        );
        validator.initializeSchema();
        logger.info("Schema validation completed");
    }

    private void cleanup() {
        threadPoolManager.close();
        iotdbSessionPool.close();
        logger.info("Application completed successfully");
    }
}
