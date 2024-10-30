package org.kreps.druidtoiotdb.utils;

import org.kreps.druidtoiotdb.model.FailedWrite;
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

public class FailedWriteLogger {
    private static final Logger logger = LoggerFactory.getLogger(FailedWriteLogger.class);
    private static final String FAILED_WRITES_DIR = "failed_writes";
    private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void logFailedWrite(FailedWrite failedWrite) {
        try {
            Path dirPath = Paths.get(FAILED_WRITES_DIR);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            String fileName = String.format("failed_writes_%s.txt",
                    LocalDateTime.now().format(FILE_DATE_FORMAT));
            Path filePath = dirPath.resolve(fileName);

            try (PrintWriter writer = new PrintWriter(new FileWriter(filePath.toString(), true))) {
                writer.println(failedWrite.toString());
            }
        } catch (IOException e) {
            logger.error("Failed to log failed write: {}", e.getMessage());
        }
    }
}
