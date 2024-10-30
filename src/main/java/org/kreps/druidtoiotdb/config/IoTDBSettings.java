package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IoTDBSettings {
    @JsonProperty("host")
    private String host;

    @JsonProperty("port")
    private int port;

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("session_pool_size")
    private int sessionPoolSize;

    // Getters and setters
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSessionPoolSize() {
        return sessionPoolSize;
    }

    public void setSessionPoolSize(int sessionPoolSize) {
        this.sessionPoolSize = sessionPoolSize;
    }

    public void validate() throws ConfigValidationException {
        if (host == null || host.isEmpty()) {
            throw new ConfigValidationException("'iotdb_settings.host' is missing or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new ConfigValidationException("'iotdb_settings.port' is invalid. It must be between 1 and 65535");
        }
        if (username == null || username.isEmpty()) {
            throw new ConfigValidationException("'iotdb_settings.username' is missing or empty");
        }
        if (password == null || password.isEmpty()) {
            throw new ConfigValidationException("'iotdb_settings.password' is missing or empty");
        }
        if (sessionPoolSize <= 0) {
            throw new ConfigValidationException(
                    "'iotdb_settings.session_pool_size' is invalid. It must be greater than 0");
        }
    }
}
