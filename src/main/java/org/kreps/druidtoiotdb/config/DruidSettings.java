package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidSettings {

    @JsonProperty("api_url")
    private String apiUrl;

    @JsonProperty("user_key")
    private String userKey;

    // Getters and setters
    public String getApiUrl() {
        return apiUrl;
    }

    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    public String getUserKey() {
        return userKey;
    }

    public void setUserKey(String userKey) {
        this.userKey = userKey;
    }

    public void validate() throws ConfigValidationException {
        if (apiUrl == null || apiUrl.isEmpty()) {
            throw new ConfigValidationException("'druid_settings.api_url' is missing or empty");
        }
        if (userKey == null || userKey.isEmpty()) {
            throw new ConfigValidationException("'druid_settings.user_key' is missing or empty");
        }
    }
}
