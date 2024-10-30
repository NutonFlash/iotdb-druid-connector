package org.kreps.druidtoiotdb.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FailedRequest {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String tag;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;
    private final String reason;
    private final int statusCode;
    private final boolean includeFullMessage;

    public FailedRequest(String tag, LocalDateTime startTime, LocalDateTime endTime, String reason, int statusCode) {
        this(tag, startTime, endTime, reason, statusCode, false);
    }

    public FailedRequest(String tag, LocalDateTime startTime, LocalDateTime endTime, String reason, int statusCode,
            boolean includeFullMessage) {
        this.tag = tag;
        this.startTime = startTime;
        this.endTime = endTime;
        this.reason = reason;
        this.statusCode = statusCode;
        this.includeFullMessage = includeFullMessage;
    }

    @Override
    public String toString() {
        if (includeFullMessage) {
            return String.format("%s|%s|%s|%d|%s",
                    tag,
                    startTime.format(FORMATTER),
                    endTime.format(FORMATTER),
                    statusCode,
                    reason);
        } else {
            // For server errors, only include the status code and basic message
            return String.format("%s|%s|%s|%d|HTTP %d error",
                    tag,
                    startTime.format(FORMATTER),
                    endTime.format(FORMATTER),
                    statusCode,
                    statusCode);
        }
    }
}
