package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceConfig {
    @JsonProperty("druid")
    private DruidSettings druidSettings;

    @JsonProperty("time_range")
    private TimeRange timeRange;

    @JsonProperty("tags_file")
    private String tagsFile;

    // Getters
    public DruidSettings getDruidSettings() {
        return druidSettings;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public String getTagsFile() {
        return tagsFile;
    }

    public void validate() throws ConfigValidationException {
        if (druidSettings == null) {
            throw new ConfigValidationException("'source.druid' section is missing");
        }
        if (timeRange == null) {
            throw new ConfigValidationException("'source.time_range' section is missing");
        }
        if (tagsFile == null || tagsFile.isEmpty()) {
            throw new ConfigValidationException("'source.tags_file' is missing or empty");
        }

        druidSettings.validate();
        timeRange.validate();
    }
}
