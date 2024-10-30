package org.kreps.druidtoiotdb.utils;

import org.kreps.druidtoiotdb.model.FailedRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FailedRequestLogger {
    private static final Logger logger = LoggerFactory.getLogger(FailedRequestLogger.class);
    private static final String FAILED_REQUESTS_DIR = "failed_requests";
    private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void logFailedRequest(FailedRequest failedRequest) {
        try {
            // Create directory if it doesn't exist
            Path dirPath = Paths.get(FAILED_REQUESTS_DIR);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            // Create file name with current date
            String fileName = String.format("failed_requests_%s.txt",
                    LocalDateTime.now().format(FILE_DATE_FORMAT));
            Path filePath = dirPath.resolve(fileName);

            // Append failed request to file
            try (PrintWriter writer = new PrintWriter(new FileWriter(filePath.toString(), true))) {
                writer.println(failedRequest.toString());
            }
        } catch (IOException e) {
            logger.error("Failed to log failed request: {}", e.getMessage());
        }
    }
}
