package us.dot.its.jpo.deduplicator.deduplicator.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;

public class JsonSerdes {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerdes.class);

    public static Serde<OdeMessageFrameData> OdeMessageFrame(DeduplicatorProperties props) {
        return Serdes.serdeFrom(
                new JsonSerializer<OdeMessageFrameData>(),
                new JsonDeserializer<>(OdeMessageFrameData.class));
    }

    public static Serde<OdeMessageFrameData> OdeMessageFrame() {
        return OdeMessageFrame(null);
    }

    public static Serde<OdeMessageFrameData> OdeMessageFrameData() {
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(OdeMessageFrameData.class));
    }
}
