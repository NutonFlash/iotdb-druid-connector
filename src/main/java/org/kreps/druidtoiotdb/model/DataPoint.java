package org.kreps.druidtoiotdb.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DataPoint {
    private static final String PREFIX = "root.cepco";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String plantCode;
    private final String orgTag;
    private final LocalDateTime oriTime;
    private final Map<String, Object> measurements;

    // Static poison pill instance
    public static final DataPoint POISON_PILL = new DataPoint(1);

    // Method to check if a data point is a poison pill
    public boolean isPoisonPill() {
        return this == POISON_PILL;
    }

    public DataPoint(int poisonPill) {
        this.plantCode = null;
        this.orgTag = null;
        this.oriTime = null;
        this.measurements = null;
    }

    public DataPoint(Map<String, String> druidData) {
        this.plantCode = druidData.get("PlantCode");
        this.orgTag = druidData.get("org_tag");
        this.oriTime = LocalDateTime.parse(druidData.get("OriTime"), DATE_FORMAT);

        // Initialize measurements map with all relevant fields
        this.measurements = new HashMap<>();
        measurements.put("Qual", druidData.get("Qual"));
        measurements.put("ColTime", druidData.get("ColTime"));
        measurements.put("std_tag", druidData.get("std_tag"));
        measurements.put("SensorType", druidData.get("SensorType"));
        measurements.put("Val", druidData.get("Val"));
    }

    public String getTimeseriesPath() {
        return String.format("%s.`%s`.`%s`", PREFIX, plantCode, orgTag.trim());
    }

    public long getTimestamp() {
        return oriTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public Map<String, Object> getMeasurements() {
        return measurements;
    }

    public String getPlantCode() {
        return plantCode;
    }

    public String getOrgTag() {
        return orgTag;
    }

    public LocalDateTime getOriTime() {
        return oriTime;
    }

    @Override
    public String toString() {
        return String.format("DataPoint{path=%s, time=%s, measurements=%s}",
                getTimeseriesPath(), oriTime, measurements);
    }
}
