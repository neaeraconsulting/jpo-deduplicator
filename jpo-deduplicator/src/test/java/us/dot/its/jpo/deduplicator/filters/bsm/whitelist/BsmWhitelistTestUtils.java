package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessage;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.Common.BSMcoreData;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;

/**
 * Construct mock properties objects for tests
 */
@Slf4j
public class BsmWhitelistTestUtils {

    public static BsmWhitelistProperties getBsmWhitelistProperties() {
        var props = new BsmWhitelistProperties();
        props.setEnabled(true);
        props.setInputTopic("topic.OdeBsmJson");
        props.setOutputDlqTopic("topic.BlacklistedOdeBsmJson");
        props.setOutputTopic("topic.WhitelistedOdeBsmJson");
        var groups = new HashMap<String, BsmWhitelistGroup>();
        var busGroup = new BsmWhitelistGroup();
        busGroup.setDescription("City Buses");
        busGroup.setIdLsb(Set.of(0x0100, 0x0123, 0x1234, 3051));
        var plowGroup = new BsmWhitelistGroup();
        plowGroup.setDescription("DOT snow plows");
        plowGroup.setIdLsb(Set.of(0x0200, 0x0234, 0x2002));
        groups.put("bus", busGroup);
        groups.put("plow", plowGroup);
        props.setGroups(groups);
        return props;
    }

    public static void setId(OdeMessageFrameData frameData, String hexId) {
        var mf = frameData.getPayload().getData();
        if (!(mf instanceof BasicSafetyMessageMessageFrame bsmMf)) {
            throw new IllegalArgumentException(String.format(
                    "Frame data %S is not of type BasicSafetyMessageMessageFrame.", mf));
        }
        TemporaryID id = new TemporaryID();
        id.setValue(hexId);
        bsmMf.getValue().getCoreData().setId(id);
    }

    public static OdeMessageFrameData getBsmMessageFrameData(String hexId) throws JsonProcessingException {
        String json = loadResource("/json/ode_bsm/sample.ode-bsm-reference.json");
        ObjectMapper mapper = DateJsonMapper.getInstance();
        OdeMessageFrameData data = mapper.readValue(json, OdeMessageFrameData.class);
        setId(data, hexId);
        return data;
    }

    public static String loadResource(String path) {
        String str;
        try {
            str = IOUtils.resourceToString(path, StandardCharsets.UTF_8);
            log.debug("Loaded resource: {}", str);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return str;
    }

    public static final Set<String> includeIds = Set.of(
            "11110100", "ffff0123", "00331234", "11110200", "11110234", "11112002", "FA820BEB",
        "97D40BEB");

    public static final Set<String> excludeIds = Set.of(
            "11113003", "00002222", "00000000", "FA820CCC",
        "97D40CCC");


}
