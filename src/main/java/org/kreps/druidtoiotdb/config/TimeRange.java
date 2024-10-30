package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeRange {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @JsonProperty("start")
    private String start;

    @JsonProperty("end")
    private String end;

    private LocalDateTime startTime;
    private LocalDateTime endTime;

    // Getters
    public LocalDateTime getStartTime() {
        if (startTime == null) {
            startTime = LocalDateTime.parse(start, FORMATTER);
        }
        return startTime;
    }

    public LocalDateTime getEndTime() {
        if (endTime == null) {
            endTime = LocalDateTime.parse(end, FORMATTER);
        }
        return endTime;
    }

    public void validate() throws ConfigValidationException {
        if (start == null || start.isEmpty()) {
            throw new ConfigValidationException("'source.time_range.start' is missing or empty");
        }
        if (end == null || end.isEmpty()) {
            throw new ConfigValidationException("'source.time_range.end' is missing or empty");
        }

        try {
            LocalDateTime startDt = getStartTime();
            LocalDateTime endDt = getEndTime();
            if (endDt.isBefore(startDt)) {
                throw new ConfigValidationException("End time must be after start time");
            }
        } catch (Exception e) {
            throw new ConfigValidationException("Invalid time format. Use ISO format (e.g., 2024-01-01T00:00:00)");
        }
    }
}
