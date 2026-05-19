package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static us.dot.its.jpo.deduplicator.filters.bsm.whitelist.BsmWhitelistTestUtils.*;

@Slf4j
@RunWith(Parameterized.class)
public class BsmWhitelistPropertiesTest {

    public BsmWhitelistPropertiesTest(TemporaryID id, boolean expectWhitelisted) {
        this.id = id;
        this.expectWhitelisted = expectWhitelisted;
    }

    final TemporaryID id;
    final boolean expectWhitelisted;

    static BsmWhitelistProperties properties;

    @BeforeClass
    public static void setUpClass() throws Exception {
        properties = getBsmWhitelistProperties();
    }

    @Test
    public void testWhitelisted() {
        boolean result = properties.whitelisted(id);
        assertThat(result, equalTo(expectWhitelisted));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        var params = new ArrayList<Object[]>();
        for (String hexId : includeIds) {
            addParams(params, hexId, true);
        }
        for (String hexId : excludeIds) {
            addParams(params, hexId, false);
        }
        return params;
    }


    private static void addParams(ArrayList<Object[]> params, String hexId, boolean expectWhitelisted) {
        var tempId = new TemporaryID();
        tempId.setValue(hexId);
        log.info("tempId: {}, value: {}, num: {}", tempId.toString(), tempId.getValue(), tempId.getOctets());
        params.add(new Object[] { tempId, expectWhitelisted} );
    }
}
