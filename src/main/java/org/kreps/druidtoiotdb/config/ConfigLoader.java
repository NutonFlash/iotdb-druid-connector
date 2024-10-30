package org.kreps.druidtoiotdb.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;

public class ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String CONFIG_FILE_PATH = "config.json";

    public static AppConfig loadConfig() throws IOException, ConfigValidationException {
        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = new File(CONFIG_FILE_PATH);

        if (!configFile.exists()) {
            throw new ConfigValidationException("Config file not found: " + CONFIG_FILE_PATH);
        }

        AppConfig config = objectMapper.readValue(configFile, AppConfig.class);

        // Validate the config
        config.validate();

        logger.info("Loading tags from CSV file: {}", config.getSourceConfig().getTagsFile());
        try {
            TagCsvParser tagParser = new TagCsvParser(config.getSourceConfig().getTagsFile());
            config.setTags(tagParser.getTags());
            tagParser.printTagsSummary();
        } catch (IOException e) {
            throw new ConfigValidationException("Failed to load tags from CSV file: " + e.getMessage());
        }

        return config;
    }
}
