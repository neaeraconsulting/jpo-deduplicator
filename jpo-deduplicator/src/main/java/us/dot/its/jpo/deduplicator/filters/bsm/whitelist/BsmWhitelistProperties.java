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

    /**
     * Enable/disable the BSM whitelist filter.
     */
    private boolean enabled;

    /**
     * Enable/disable sending the TempID of discarded/blacklisted messages to a Dead Letter Queue
     * (DLQ) topic.
     */
    private boolean dlqEnabled;

    /**
     * Input topic with unfiltered BSMs.
     */
    private String inputTopic;

    /**
     * Output topic for whitelisted BSMs.
     */
    private String outputTopic;

    /**
     * Dead Letter Queue topic with the TemID of discarded/blacklisted messages.
     */
    private String outputDlqTopic;

    /**
     * Map of BSM whitelist groups.
     */
    private Map<String, BsmWhitelistGroup> groups;


    /**
     * Test if the least significant bits of an id are whitelisted
     */
    public boolean whitelisted(TemporaryID id) {
        if (id == null) {
            log.debug("TemporaryID is null");
            return false;
        }
        byte[] idBytes = id.getOctets();
        if (idBytes.length != 4) {
            log.debug ("Malformed TemporaryID {} doesn't have 4 octets", id);
            return false;
        }
        int lsb = (Byte.toUnsignedInt(idBytes[2]) << 8) | Byte.toUnsignedInt(idBytes[3]);
        if (groups == null) return false;
        for (BsmWhitelistGroup group : groups.values()) {
            if (group.getIdLsb() != null && group.getIdLsb().contains(lsb)) {
                return true;
            }
        }
        return false;
    }
}
