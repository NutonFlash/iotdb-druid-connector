package org.kreps.druidtoiotdb.threading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThreadPoolManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    private final ExecutorService fetcherPool;
    private final ExecutorService writerPool;
    private final CountDownLatch fetcherCompletionLatch;
    private final CountDownLatch writerCompletionLatch;

    public ThreadPoolManager(int readerPoolSize, int writerPoolSize) {
        this.fetcherPool = Executors.newFixedThreadPool(readerPoolSize);
        this.writerPool = Executors.newFixedThreadPool(writerPoolSize);
        this.fetcherCompletionLatch = new CountDownLatch(readerPoolSize);
        this.writerCompletionLatch = new CountDownLatch(writerPoolSize);
    }

    public ExecutorService getFetcherPool() {
        return fetcherPool;
    }

    public ExecutorService getWriterPool() {
        return writerPool;
    }

    public CountDownLatch getFetcherLatch() {
        return fetcherCompletionLatch;
    }

    public CountDownLatch getWriterLatch() {
        return writerCompletionLatch;
    }

    public void waitForFetchers() throws InterruptedException {
        logger.info("Waiting for fetchers to complete...");
        fetcherCompletionLatch.await();
        logger.info("All fetchers completed");
    }

    public void waitForWriters() throws InterruptedException {
        logger.info("Waiting for writers to complete...");
        writerCompletionLatch.await();
        logger.info("All writers completed");
    }

    @Override
    public void close() {
        logger.info("Shutting down thread pools...");

        // Shutdown fetcher pool first
        fetcherPool.shutdown();
        try {
            if (!fetcherPool.awaitTermination(30, TimeUnit.SECONDS)) {
                fetcherPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            fetcherPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Then shutdown writer pool
        writerPool.shutdown();
        try {
            if (!writerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                writerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            writerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Thread pools shutdown completed");
    }
}
