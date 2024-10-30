package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class AppConfig {

    @JsonProperty("source")
    private SourceConfig sourceConfig;

    @JsonProperty("destination")
    private DestinationConfig destinationConfig;

    @JsonProperty("processing")
    private ProcessingConfig processingConfig;

    @JsonProperty("retry")
    private RetryConfig retryConfig;

    private List<String> tags; // Loaded tags from CSV

    // Getters
    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public DestinationConfig getDestinationConfig() {
        return destinationConfig;
    }

    public ProcessingConfig getProcessingConfig() {
        return processingConfig;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public RetryConfig getRetryConfig() {
        return retryConfig;
    }

    public void validate() throws ConfigValidationException {
        if (sourceConfig == null) {
            throw new ConfigValidationException("'source' section is missing");
        }
        if (destinationConfig == null) {
            throw new ConfigValidationException("'destination' section is missing");
        }
        if (processingConfig == null) {
            throw new ConfigValidationException("'processing' section is missing");
        }

        sourceConfig.validate();
        destinationConfig.validate();
        processingConfig.validate();
        retryConfig.validate();
    }
}
