package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import java.util.HashSet;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static us.dot.its.jpo.deduplicator.filters.bsm.whitelist.BsmWhitelistTestUtils.*;
import static us.dot.its.jpo.deduplicator.utils.OdeJsonUtils.getBsmTemporaryID;

public class BsmWhitelistTopologyTest {

    @Test
    public void testTopology() throws JsonProcessingException {
        DeduplicatorProperties dedupProps = new DeduplicatorProperties();
        BsmWhitelistProperties props = getBsmWhitelistProperties();
        var whitelistTopology = new BsmWhitelistTopology(dedupProps, props);
        Topology topology = whitelistTopology.buildTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            final TestInputTopic<Void, OdeMessageFrameData> inputTopic =
                driver.createInputTopic(props.getInputTopic(),
                    new VoidSerializer(),
                    new JsonSerializer<OdeMessageFrameData>());

            final TestOutputTopic<Void, OdeMessageFrameData> outputTopic =
                driver.createOutputTopic(props.getOutputTopic(),
                    new VoidDeserializer(),
                    new JsonDeserializer<>(OdeMessageFrameData.class));

            final TestOutputTopic<Void, String> dlqTopic =
                driver.createOutputTopic(props.getOutputDlqTopic(),
                    new VoidDeserializer(),
                    new StringDeserializer());

            for (String id : includeIds) {
                inputTopic.pipeInput(getBsmMessageFrameData(id));
            }
            for (String id : excludeIds) {
                inputTopic.pipeInput(getBsmMessageFrameData(id));
            }

            List<OdeMessageFrameData> dataList = outputTopic.readValuesToList();

            // Result should include only ids in "include" list
            assertThat(dataList, hasSize(includeIds.size()));
            Set<String> resultIds = dataList.stream()
                    .map(data -> getBsmTemporaryID(data).toString().toLowerCase())
                    .collect(Collectors.toSet());
            Set<String> diff = Sets.symmetricDifference(resultIds, includeIds);
            assertThat(
                String.format("includeIds: %s, resultIds: %s, diff: %s", includeIds, resultIds, diff),
                diff, hasSize(0));

            // DLQ topic should have all excluded messages
            List<String> dlqList = dlqTopic.readValuesToList();
            assertThat(dlqList, hasSize(excludeIds.size()));
            Set<String> dlqIds = new HashSet<>(dlqList);
            Set<String> dlqDiff = Sets.symmetricDifference(dlqIds, excludeIds);
            assertThat(
                String.format("excludeIds: %s, dlqIds: %s, diff: %s", excludeIds, dlqIds, dlqDiff),
                dlqDiff, hasSize(0));

        }
    }



}
