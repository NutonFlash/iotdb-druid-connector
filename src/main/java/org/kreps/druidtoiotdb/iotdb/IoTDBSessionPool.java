package org.kreps.druidtoiotdb.iotdb;

import org.apache.iotdb.session.pool.SessionPool;
import org.kreps.druidtoiotdb.config.AppConfig;
import org.kreps.druidtoiotdb.config.IoTDBSettings;
import org.kreps.druidtoiotdb.utils.RetryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBSessionPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IoTDBSessionPool.class);

    private SessionPool sessionPool;

    private static final int CONNECTION_CHECK_INTERVAL_MS = 5000;
    private volatile boolean isAvailable = true;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private Thread connectionMonitorThread;
    private final AppConfig config;

    public IoTDBSessionPool(AppConfig config) {
        logger.info("Initializing IoTDB SessionPool with pool size: {}",
                config.getDestinationConfig().getIotdbSettings().getSessionPoolSize());
        this.config = config;
        initializeSessionPool(config.getDestinationConfig().getIotdbSettings());
        startConnectionMonitor();
    }

    private void initializeSessionPool(IoTDBSettings settings) {
        try {
            this.sessionPool = new SessionPool.Builder()
                    .host(settings.getHost())
                    .port(settings.getPort())
                    .user(settings.getUsername())
                    .password(settings.getPassword())
                    .maxSize(settings.getSessionPoolSize())
                    .build();
            isAvailable = true;
            logger.info("IoTDB SessionPool initialized successfully");
        } catch (Exception e) {
            isAvailable = false;
            logger.error("Failed to initialize IoTDB SessionPool: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private synchronized void reinitializeSessionPool() {
        logger.info("Reinitializing IoTDB SessionPool...");
        if (sessionPool != null) {
            try {
                sessionPool.close();
            } catch (Exception e) {
                logger.warn("Error while closing old session pool: {}", e.getMessage());
            }
        }

        IoTDBSettings settings = config.getDestinationConfig().getIotdbSettings();
        try {
            this.sessionPool = new SessionPool.Builder()
                    .host(settings.getHost())
                    .port(settings.getPort())
                    .user(settings.getUsername())
                    .password(settings.getPassword())
                    .maxSize(settings.getSessionPoolSize())
                    .build();
            isAvailable = true;
            logger.info("IoTDB SessionPool reinitialized successfully");
        } catch (Exception e) {
            isAvailable = false;
            logger.error("Failed to reinitialize IoTDB SessionPool: {}", e.getMessage());
        }
    }

    public SessionPool getSessionPool() {
        return sessionPool;
    }

    public boolean checkConnection() {
        try {
            return RetryUtils.executeWithRetry(() -> {
                try {
                    sessionPool.executeQueryStatement("show databases");
                    if (!isAvailable) {
                        logger.info("IoTDB connection restored");
                        isAvailable = true;
                    }
                    return true;
                } catch (Exception e) {
                    if (e.getMessage().contains("timeout to get a connection")) {
                        logger.warn("Connection pool timeout, attempting to reinitialize...");
                        reinitializeSessionPool();
                        throw e; // Let retry mechanism handle it
                    }
                    throw e;
                }
            }, config.getRetryConfig(), "IoTDB connection check");
        } catch (Exception e) {
            if (isAvailable) {
                logger.error("IoTDB connection lost: {}", e.getMessage());
                isAvailable = false;
            }
            return false;
        }
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    private void startConnectionMonitor() {
        connectionMonitorThread = new Thread(() -> {
            while (!shutdownInitiated.get()) {
                if (!checkConnection()) {
                    logger.error("IoTDB connection is not available");
                }
                try {
                    Thread.sleep(CONNECTION_CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "IoTDB-Connection-Monitor");
        connectionMonitorThread.setDaemon(true);
        connectionMonitorThread.start();
    }

    @Override
    public void close() {
        shutdownInitiated.set(true);
        if (connectionMonitorThread != null) {
            connectionMonitorThread.interrupt();
        }
        if (sessionPool != null) {
            logger.info("Closing IoTDB SessionPool");
            sessionPool.close();
            logger.info("IoTDB SessionPool closed successfully");
        }
    }
}
