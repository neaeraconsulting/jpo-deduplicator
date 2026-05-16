package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "filters.bsm.whitelist")
public class BsmWhitelistProperties {
    private boolean enabled;
    private String inputTopic;
    private String outputTopic;
    private Map<String, BsmWhitelistGroup> groups;
}
