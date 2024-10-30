package org.kreps.druidtoiotdb.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TagCsvParser {

    private static final Logger logger = LoggerFactory.getLogger(TagCsvParser.class);
    private final List<String> tags;

    /**
     * Constructor that parses tags from a specified CSV file path.
     *
     * @param filePath The path to the CSV file containing the tags.
     * @throws IOException If an error occurs during file reading.
     */
    public TagCsvParser(String filePath) throws IOException {
        File csvFile = new File(filePath);
        if (!csvFile.exists()) {
            throw new IOException("Tags CSV file not found: " + filePath);
        }
        this.tags = parseTagsFromCsv(filePath);
    }

    /**
     * Parses the tags from a single line CSV file.
     *
     * @param filePath The path to the CSV file.
     * @return A list of tags parsed from the file.
     * @throws IOException If an error occurs during file reading.
     */
    private List<String> parseTagsFromCsv(String filePath) throws IOException {
        List<String> parsedTags = new ArrayList<>();
        int lineNumber = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                List<String> lineTags = parseLine(line, lineNumber);
                parsedTags.addAll(lineTags);
            }
        }

        if (parsedTags.isEmpty()) {
            throw new IOException("No valid tags found in the CSV file.");
        } else {
            logger.info("Successfully parsed {} tags from the CSV file.", parsedTags.size());
        }

        return parsedTags;
    }

    private List<String> parseLine(String line, int lineNumber) {
        List<String> lineTags = new ArrayList<>();
        String[] rawTags = line.split(",");

        for (String tag : rawTags) {
            String trimmedTag = tag.trim();
            if (!trimmedTag.isEmpty()) {
                lineTags.add(trimmedTag);
            } else {
                logger.warn("Empty tag found on line {}. Skipping.", lineNumber);
            }
        }

        return lineTags;
    }

    /**
     * Returns the list of parsed tags.
     *
     * @return The list of tags.
     */
    public List<String> getTags() {
        return tags;
    }

    public void printTagsSummary() {
        logger.info("Total number of parsed tags: {}", tags.size());
        logger.info("First 5 tags: {}", tags.stream().limit(5).collect(Collectors.joining(", ")));
        if (tags.size() > 5) {
            logger.info("Last 5 tags: {}",
                    tags.stream().skip(Math.max(0, tags.size() - 5)).collect(Collectors.joining(", ")));
        }
    }
}
