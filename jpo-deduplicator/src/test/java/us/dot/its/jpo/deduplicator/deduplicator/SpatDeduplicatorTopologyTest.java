package us.dot.its.jpo.deduplicator.deduplicator;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.SpatDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class SpatDeduplicatorTopologyTest {

    String inputTopic = "topic.OdeSpatJson";
    String outputTopic = "topic.DeduplicatedOdeSpatJson";
    ObjectMapper objectMapper;

    // Reference SPaT message
    String inputSpat1;

    // Same as SPaT 1 (duplicate within 500ms) - should be dropped
    String inputSpat2;

    // SPaT 1 but 1 second later - should be forwarded
    String inputSpat3;

    @Autowired
    DeduplicatorProperties props;

    @Before
    public void setup() throws IOException {
        objectMapper = DateJsonMapper.getInstance();

        String spatReference = new String(
                Files.readAllBytes(Paths.get("src/test/resources/json/ode_spat/sample.ode-spat-reference.json")));
        OdeMessageFrameData spatReferenceData = objectMapper.readValue(spatReference, OdeMessageFrameData.class);

        // Reference SPaT - should be forwarded
        inputSpat1 = spatReferenceData.toJson();

        // Duplicate - same time, should be dropped
        inputSpat2 = spatReferenceData.toJson();

        // 1 second later - should be forwarded (beyond 500ms window)
        OdeMessageFrameData spat1SecondLater = objectMapper.readValue(spatReferenceData.toJson(),
                OdeMessageFrameData.class);
        String originalTime = spat1SecondLater.getMetadata().getOdeReceivedAt();
        Instant instant = Instant.parse(originalTime);
        spat1SecondLater.getMetadata().setOdeReceivedAt(instant.plus(1, ChronoUnit.SECONDS).toString());
        inputSpat3 = spat1SecondLater.toJson();
    }

    @Test
    public void testSerialization() throws JsonMappingException, JsonProcessingException {
        OdeMessageFrameData spat = objectMapper.readValue(inputSpat1, OdeMessageFrameData.class);
        String json = objectMapper.writeValueAsString(spat);
        assertEquals(inputSpat1, json);
    }

    @Test
    public void testJsonSerdes() {
        Serde<OdeMessageFrameData> serdes = JsonSerdes.OdeMessageFrame();
        OdeMessageFrameData deserialized = serdes.deserializer().deserialize(null, inputSpat1.getBytes());
        byte[] serialized = serdes.serializer().serialize(null, deserialized);
        assertThat(new String(serialized), jsonEquals(inputSpat1));
    }

    @Test
    public void testTopology() {
        props = new DeduplicatorProperties();
        props.setKafkaTopicOdeSpatJson(inputTopic);
        props.setKafkaTopicDeduplicatedOdeSpatJson(outputTopic);

        SpatDeduplicatorTopology spatDeduplicatorTopology = new SpatDeduplicatorTopology(props);
        Topology topology = spatDeduplicatorTopology.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<Void, String> inputOdeSpatData = driver.createInputTopic(
                    inputTopic,
                    Serdes.Void().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, OdeMessageFrameData> outputOdeSpatData = driver.createOutputTopic(
                    outputTopic,
                    Serdes.String().deserializer(),
                    JsonSerdes.OdeMessageFrame().deserializer());

            inputOdeSpatData.pipeInput(null, inputSpat1);
            inputOdeSpatData.pipeInput(null, inputSpat2);
            inputOdeSpatData.pipeInput(null, inputSpat3);

            List<KeyValue<String, OdeMessageFrameData>> spatDeduplicationResults = outputOdeSpatData
                    .readKeyValuesToList();

            // spat1 forwarded (first), spat2 dropped (duplicate), spat3 forwarded (1s later)
            assertEquals(2, spatDeduplicationResults.size());

            OdeMessageFrameData spat1 = objectMapper.readValue(inputSpat1, OdeMessageFrameData.class);
            OdeMessageFrameData spat3 = objectMapper.readValue(inputSpat3, OdeMessageFrameData.class);

            assertThat(spatDeduplicationResults.get(0).value.toJson(), jsonEquals(spat1.toJson()));
            assertThat(spatDeduplicationResults.get(1).value.toJson(), jsonEquals(spat3.toJson()));

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
