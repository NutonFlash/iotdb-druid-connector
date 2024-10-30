package org.kreps.druidtoiotdb.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;

public class TagDistributor {
    private static final Logger logger = LoggerFactory.getLogger(TagDistributor.class);

    public static List<List<String>> distributeTags(List<String> tags, int fetcherCount) {
        List<List<String>> distribution = new ArrayList<>(fetcherCount);
        for (int i = 0; i < fetcherCount; i++) {
            distribution.add(new ArrayList<>());
        }

        for (int i = 0; i < tags.size(); i++) {
            distribution.get(i % fetcherCount).add(tags.get(i));
        }

        logDistribution(distribution);
        return distribution;
    }

    private static void logDistribution(List<List<String>> distribution) {
        for (int i = 0; i < distribution.size(); i++) {
            logger.info("Fetcher-{} assigned {} tags", i + 1, distribution.get(i).size());
        }
    }
}
