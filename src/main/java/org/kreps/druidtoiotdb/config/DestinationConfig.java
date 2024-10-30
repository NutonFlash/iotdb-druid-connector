package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DestinationConfig {
    @JsonProperty("iotdb")
    private IoTDBSettings iotdbSettings;

    public IoTDBSettings getIotdbSettings() {
        return iotdbSettings;
    }

    public void validate() throws ConfigValidationException {
        if (iotdbSettings == null) {
            throw new ConfigValidationException("'destination.iotdb' section is missing");
        }
        iotdbSettings.validate();
    }
}
