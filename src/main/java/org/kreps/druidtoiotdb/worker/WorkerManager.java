package org.kreps.druidtoiotdb.worker;

import org.kreps.druidtoiotdb.config.AppConfig;
import org.kreps.druidtoiotdb.model.DataPoint;
import org.kreps.druidtoiotdb.fetcher.DataFetcher;
import org.kreps.druidtoiotdb.iotdb.IoTDBSessionPool;
import org.kreps.druidtoiotdb.writer.IoTDBWriter;
import org.kreps.druidtoiotdb.threading.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

    private final AppConfig config;
    private final BlockingQueue<DataPoint> dataQueue;
    private final ThreadPoolManager threadPoolManager;
    private final IoTDBSessionPool iotdbSessionPool;

    // Lists to keep track of workers
    private final List<IoTDBWriter> writers = new ArrayList<>();
    private final List<DataFetcher> fetchers = new ArrayList<>();

    private volatile boolean shutdownInProgress = false;

    public WorkerManager(AppConfig config, BlockingQueue<DataPoint> dataQueue,
            ThreadPoolManager threadPoolManager, IoTDBSessionPool iotdbSessionPool) {
        this.config = config;
        this.dataQueue = dataQueue;
        this.threadPoolManager = threadPoolManager;
        this.iotdbSessionPool = iotdbSessionPool;
    }

    public void startWorkers() {
        startWriters();
        startFetchers();
    }

    private void startWriters() {
        int writerPoolSize = config.getProcessingConfig().getThreads().getWriterPoolSize();
        logger.info("Starting {} writer threads...", writerPoolSize);

        for (int i = 0; i < writerPoolSize; i++) {
            IoTDBWriter writer = new IoTDBWriter(
                    config,
                    dataQueue,
                    iotdbSessionPool,
                    threadPoolManager.getWriterLatch(),
                    this,
                    i + 1);
            writers.add(writer);
            threadPoolManager.getWriterPool().submit(writer);
        }
    }

    private void startFetchers() {
        int readerPoolSize = config.getProcessingConfig().getThreads().getReaderPoolSize();
        logger.info("Starting {} fetcher threads...", readerPoolSize);

        List<List<String>> tagDistribution = TagDistributor.distributeTags(
                config.getTags(),
                readerPoolSize);

        for (int i = 0; i < readerPoolSize; i++) {
            DataFetcher fetcher = new DataFetcher(
                    config,
                    dataQueue,
                    tagDistribution.get(i),
                    i + 1,
                    threadPoolManager.getFetcherLatch(),
                    this);
            fetchers.add(fetcher);
            threadPoolManager.getFetcherPool().submit(fetcher);
        }
    }

    /**
     * Initiates a graceful shutdown of the application.
     * - Stops all fetchers and writers.
     * - Sends poison pills to writers.
     * - Waits for all workers to complete.
     */
    public synchronized void initiateShutdown() {
        if (shutdownInProgress) {
            logger.info("Shutdown already in progress");
            return;
        }

        shutdownInProgress = true;
        logger.info("Initiating graceful shutdown...");

        // Stop all fetchers first
        logger.info("Stopping {} fetchers...", fetchers.size());
        fetchers.forEach(DataFetcher::stop);

        // Stop all writers
        logger.info("Sending poison pills to {} writers...", writers.size());
        writers.forEach(writer -> dataQueue.add(DataPoint.POISON_PILL));

        // Create a separate thread for shutdown to avoid deadlock
        Thread shutdownThread = new Thread(() -> {
            try {
                threadPoolManager.waitForFetchers();
                threadPoolManager.waitForWriters();
                cleanup();
            } catch (InterruptedException e) {
                logger.error("Shutdown process was interrupted", e);
                Thread.currentThread().interrupt();
            }
        }, "ShutdownThread");

        shutdownThread.start();
    }

    private void cleanup() {
        threadPoolManager.close();
        logger.info("Graceful shutdown completed");
    }

    /**
     * Sends poison pills to all writer threads to signal them to terminate.
     */
    public void sendPoisonPills() {
        logger.info("Sending poison pills to writers...");
        int writerPoolSize = config.getProcessingConfig().getThreads().getWriterPoolSize();
        for (int i = 0; i < writerPoolSize; i++) {
            try {
                dataQueue.put(DataPoint.POISON_PILL);
            } catch (InterruptedException e) {
                logger.error("Interrupted while sending poison pills", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}