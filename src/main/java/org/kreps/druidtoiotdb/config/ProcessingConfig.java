package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessingConfig {
    @JsonProperty("threads")
    private ThreadConfig threads;

    @JsonProperty("batch")
    private BatchConfig batch;

    @JsonProperty("queue_size")
    private int queueSize;

    // Getters
    public ThreadConfig getThreads() {
        return threads;
    }

    public BatchConfig getBatch() {
        return batch;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void validate() throws ConfigValidationException {
        if (threads == null) {
            throw new ConfigValidationException("'processing.threads' section is missing");
        }
        if (batch == null) {
            throw new ConfigValidationException("'processing.batch' section is missing");
        }
        if (queueSize <= 0) {
            throw new ConfigValidationException("'processing.queue_size' must be greater than 0");
        }

        threads.validate();
        batch.validate();
    }
}
