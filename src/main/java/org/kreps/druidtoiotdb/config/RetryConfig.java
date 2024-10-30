package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryConfig {
    @JsonProperty("initial_delay_ms")
    private long initialDelayMs;

    @JsonProperty("max_delay_ms")
    private long maxDelayMs;

    @JsonProperty("max_attempts")
    private int maxAttempts;

    @JsonProperty("backoff_multiplier")
    private double backoffMultiplier;

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void validate() throws ConfigValidationException {
        if (initialDelayMs <= 0) {
            throw new ConfigValidationException("initial_delay_ms must be positive");
        }
        if (maxDelayMs < initialDelayMs) {
            throw new ConfigValidationException("max_delay_ms must be greater than or equal to initial_delay_ms");
        }
        if (maxAttempts <= 0) {
            throw new ConfigValidationException("max_attempts must be positive");
        }
        if (backoffMultiplier <= 1.0) {
            throw new ConfigValidationException("backoff_multiplier must be greater than 1.0");
        }
    }
}
