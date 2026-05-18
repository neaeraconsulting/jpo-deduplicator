package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;

import java.util.Map;


@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "filters.bsm.whitelist")
public class BsmWhitelistProperties {

    private boolean enabled;
    private String inputTopic;
    private String outputTopic;
    private String outputDlqTopic;
    private Map<String, BsmWhitelistGroup> groups;


    /**
     * Test if the least significant bits of an id are whitelisted
     */
    public boolean whitelisted(TemporaryID id) {
        byte[] idBytes = id.getOctets();
        if (idBytes.length != 4) {
            log.warn ("Malformed TemporaryID {} doesn't have 4 octets", id);
            return false;
        }
        int lsb = (idBytes[2] << 8) | idBytes[3];
        if (groups == null) return false;
        for (BsmWhitelistGroup group : groups.values()) {
            if (group.getIdLsb() != null && group.getIdLsb().contains(lsb)) {
                return true;
            }
        }
        return false;
    }
}
