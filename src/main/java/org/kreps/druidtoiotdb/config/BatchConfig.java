package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchConfig {
    @JsonProperty("read_size")
    private int readSize;

    @JsonProperty("write_size")
    private int writeSize;

    // Getters
    public int getReadSize() {
        return readSize;
    }

    public int getWriteSize() {
        return writeSize;
    }

    public void validate() throws ConfigValidationException {
        if (readSize <= 0) {
            throw new ConfigValidationException("'processing.batch.read_size' must be greater than 0");
        }
        if (writeSize <= 0) {
            throw new ConfigValidationException("'processing.batch.write_size' must be greater than 0");
        }
    }
}
