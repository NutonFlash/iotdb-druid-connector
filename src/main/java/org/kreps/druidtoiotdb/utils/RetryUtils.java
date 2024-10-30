package org.kreps.druidtoiotdb.utils;

import org.kreps.druidtoiotdb.config.RetryConfig;
import org.kreps.druidtoiotdb.exceptions.ClientErrorException;
import org.kreps.druidtoiotdb.exceptions.ServerErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryUtils {
    private static final Logger logger = LoggerFactory.getLogger(RetryUtils.class);

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T executeWithRetry(ThrowingSupplier<T> operation, RetryConfig config, String operationName)
            throws Exception {
        long delay = config.getInitialDelayMs();
        int attempts = 0;

        while (attempts < config.getMaxAttempts()) {
            try {
                return operation.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                if (e instanceof ClientErrorException) {
                    throw e;
                }

                attempts++;
                boolean isLastAttempt = attempts >= config.getMaxAttempts();

                if (Thread.interrupted()) {
                    throw new InterruptedException("Operation interrupted between retries");
                }

                if (isLastAttempt && isCriticalError(e)) {
                    logger.error(
                            "Critical error during {}: {}. Maximum retry attempts reached, treating as server error.",
                            operationName, e.getMessage());
                    throw new ServerErrorException("Critical server error: " + e.getMessage(), 503);
                }

                if (isLastAttempt) {
                    logger.error("{} failed after {} attempts. Final error: {}",
                            operationName, attempts, e.getMessage());
                    throw e;
                }

                logger.warn("{} failed (attempt {}/{}). Error: {}",
                        operationName, attempts, config.getMaxAttempts(), e.getMessage());

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }

                delay *= 2; // Exponential backoff
            }
        }

        throw new Exception("Retry operation failed: max attempts reached");
    }

    private static boolean isCriticalError(Exception e) {
        return e.getMessage().contains("Connection refused")
                || e instanceof ServerErrorException
                || (e.getCause() != null && e.getCause() instanceof java.net.ConnectException);
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static void executeWithRetry(ThrowingRunnable operation, RetryConfig config, String operationName)
            throws Exception {
        executeWithRetry(() -> {
            operation.run();
            return null;
        }, config, operationName);
    }
}
