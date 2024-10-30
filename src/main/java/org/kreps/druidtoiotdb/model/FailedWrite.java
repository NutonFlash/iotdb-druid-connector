package org.kreps.druidtoiotdb.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;

public class FailedWrite {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String tag;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;
    private final String reason;
    private final int pointCount;
    private final String devicePath;

    public FailedWrite(String tag, String devicePath, List<DataPoint> points, String reason) {
        this.tag = tag;
        this.devicePath = devicePath;
        this.reason = reason;
        this.pointCount = points.size();

        // Sort points by timestamp first
        List<DataPoint> sortedPoints = points.stream()
                .sorted(Comparator.comparing(DataPoint::getTimestamp))
                .collect(Collectors.toList());

        // Calculate time range from sorted points
        this.startTime = LocalDateTime.ofEpochSecond(
                sortedPoints.get(0).getTimestamp() / 1000, 0, java.time.ZoneOffset.UTC);
        this.endTime = LocalDateTime.ofEpochSecond(
                sortedPoints.get(sortedPoints.size() - 1).getTimestamp() / 1000, 0, java.time.ZoneOffset.UTC);
    }

    @Override
    public String toString() {
        return String.format("%s|%s|%s|%s|%d|%s",
                tag,
                startTime.format(FORMATTER),
                endTime.format(FORMATTER),
                devicePath,
                pointCount,
                reason);
    }
}
