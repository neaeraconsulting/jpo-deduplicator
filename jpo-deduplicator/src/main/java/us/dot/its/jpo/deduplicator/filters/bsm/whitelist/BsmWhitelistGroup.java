package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import lombok.Data;

import java.util.Set;

@Data
public class BsmWhitelistGroup {

    /**
     * Short name of the group.
     * For example: "bus", "plow", "probe"
     */
    private String name;

    /**
     * Full description of the group.
     * For example: "City buses", "DOT Snow Plows", "IOO probe vehicles"
     */
    private String description;

    /**
     * Decimal representation of the two least significant bytes of the BSM
     * Temp ID that are fixed by the OBU to be whitelisted.
     */
    private Set<Integer> idLsb;
}
