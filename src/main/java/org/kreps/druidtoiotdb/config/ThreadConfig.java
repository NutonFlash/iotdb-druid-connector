package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ThreadConfig {
    @JsonProperty("reader_pool_size")
    private int readerPoolSize;

    @JsonProperty("writer_pool_size")
    private int writerPoolSize;

    // Getters
    public int getReaderPoolSize() {
        return readerPoolSize;
    }

    public int getWriterPoolSize() {
        return writerPoolSize;
    }

    public void validate() throws ConfigValidationException {
        if (readerPoolSize <= 0) {
            throw new ConfigValidationException("'processing.threads.reader_pool_size' must be greater than 0");
        }
        if (writerPoolSize <= 0) {
            throw new ConfigValidationException("'processing.threads.writer_pool_size' must be greater than 0");
        }
    }
}
