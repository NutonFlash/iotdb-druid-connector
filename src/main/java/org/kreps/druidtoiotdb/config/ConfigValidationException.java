package org.kreps.druidtoiotdb.config;

public class ConfigValidationException extends RuntimeException {
    public ConfigValidationException(String message) {
        super(message);
    }
}
