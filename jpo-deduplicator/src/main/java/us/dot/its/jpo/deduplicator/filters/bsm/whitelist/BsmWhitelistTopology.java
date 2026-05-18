package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessage;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.Common.BSMcoreData;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

@Slf4j
public class BsmWhitelistTopology {

    public static final String TOPOLOGY_NAME = "BsmWhitelist";

    @Getter
    KafkaStreams streams;
    DeduplicatorProperties deduplicatorProps;
    BsmWhitelistProperties props;
    ObjectMapper objectMapper;

    public BsmWhitelistTopology(DeduplicatorProperties deduplicatorProps,
        BsmWhitelistProperties props
    ) {
        this.deduplicatorProps = deduplicatorProps;
        this.props = props;
        this.objectMapper = DateJsonMapper.getInstance();
    }

    public void start() {
        if (streams != null && streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Start called while streams is already running.");
        }
        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, deduplicatorProps.createStreamProperties(TOPOLOGY_NAME));
        if (exceptionHandler != null)
            streams.setUncaughtExceptionHandler(exceptionHandler);
        if (stateListener != null)
            streams.setStateListener(stateListener);
        log.info("Starting {} Topology", TOPOLOGY_NAME);
        streams.start();
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(props.getInputTopic(),
                        Consumed.with(
                                Serdes.Void(),
                                JsonSerdes.OdeMessageFrameData()))

                .filter((key, frameData) -> {

                    if (frameData == null || frameData.getPayload() == null || frameData.getPayload().getData() == null) {
                        log.warn("Frame data or contents is null or empty.");
                        return false;
                    }

                    var mf = frameData.getPayload().getData();

                    if (!(mf instanceof BasicSafetyMessageMessageFrame bsmMf)) {
                        log.warn("Frame data {} is not of type BasicSafetyMessageMessageFrame.", mf);
                        return false;
                    }

                    BasicSafetyMessage bsm = bsmMf.getValue();

                    if (bsm == null) {
                        log.warn("BasicSafetyMessage is null in message frame {}.", bsmMf);
                        return false;
                    }

                    BSMcoreData coreData = bsm.getCoreData();
                    if (coreData == null) {
                        log.warn("CoreData is null in bsm {}.", bsm);
                        return false;
                    }

                    TemporaryID id = coreData.getId();
                    boolean isWhitelisted = props.whitelisted(id);
                    log.info("id {} is whitelisted", id);
                    return isWhitelisted;

                })
                .to(props.getOutputTopic(), Produced.with(
                   Serdes.Void(),
                   JsonSerdes.OdeMessageFrameData()
                ));

        return builder.build();

    }

    public void stop() {
        log.info("Stopping {} Topology.", TOPOLOGY_NAME);
        if (streams != null) {
            streams.close();
            streams.cleanUp();
            streams = null;
        }
        log.info("Stopped {} Topology.", TOPOLOGY_NAME);
    }

    KafkaStreams.StateListener stateListener;

    public void registerStateListener(KafkaStreams.StateListener stateListener) {
        this.stateListener = stateListener;
    }

    StreamsUncaughtExceptionHandler exceptionHandler;

    public void registerUncaughtExceptionHandler(StreamsUncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }
}
