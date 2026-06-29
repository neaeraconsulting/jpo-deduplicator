package us.dot.its.jpo.deduplicator.deduplicator.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPATMessageFrame;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.format.DateTimeFormatter;

import us.dot.its.jpo.deduplicator.deduplicator.processors.suppliers.OdeSpatJsonProcessorSupplier;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.deduplicator.deduplicator.serialization.JsonSerdes;

public class SpatDeduplicatorTopology {

    private static final Logger logger = LoggerFactory.getLogger(SpatDeduplicatorTopology.class);

    Topology topology;
    KafkaStreams streams;
    DeduplicatorProperties props;
    ObjectMapper objectMapper;
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    public SpatDeduplicatorTopology(DeduplicatorProperties props) {
        this.props = props;
        this.objectMapper = DateJsonMapper.getInstance();
    }

    public void start() {
        if (streams != null && streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Start called while streams is already running.");
        }
        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, props.createStreamProperties("SpatDeduplicator"));
        if (exceptionHandler != null)
            streams.setUncaughtExceptionHandler(exceptionHandler);
        if (stateListener != null)
            streams.setStateListener(stateListener);
        logger.info("Starting Spat Deduplicator Topology");
        streams.start();
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, OdeMessageFrameData> inputStream = builder.stream(props.getKafkaTopicOdeSpatJson(),
                Consumed.with(Serdes.Void(), JsonSerdes.OdeMessageFrame()));

        builder.addStateStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(props.getKafkaStateStoreOdeSpatJsonName()),
                        Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> spatRekeyedStream = inputStream.selectKey((key, value) -> {
            try {
                if (value == null || value.getPayload() == null || value.getPayload().getData() == null) {
                    logger.warn("Received SPaT message with null payload or data, discarding message");
                    return "unknown";
                }

                SPATMessageFrame mf = (SPATMessageFrame) value.getPayload().getData();
                if (mf == null || mf.getValue() == null || mf.getValue().getIntersections() == null ||
                        mf.getValue().getIntersections().isEmpty()) {
                    logger.warn("Received SPaT message with null message frame data, discarding message");
                    return "unknown";
                }

                RsuIntersectionKey newKey = new RsuIntersectionKey();
                newKey.setRsuId(((OdeMessageFrameMetadata) value.getMetadata()).getOriginIp());
                newKey.setIntersectionReferenceID(mf.getValue().getIntersections().get(0).getId());
                return newKey.toString();
            } catch (Exception e) {
                logger.error("Error extracting key from SPaT message: " + e.getMessage() + ", discarding message", e);
                return "unknown";
            }
        })
        .filter((key, value) -> {
            if ("unknown".equals(key)) {
                logger.debug("Discarding SPaT message with unknown key");
                return false;
            }
            return true;
        })
        .repartition(Repartitioned.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        KStream<String, OdeMessageFrameData> deduplicatedStream = spatRekeyedStream.process(
                new OdeSpatJsonProcessorSupplier(props.getKafkaStateStoreOdeSpatJsonName(), props),
                props.getKafkaStateStoreOdeSpatJsonName());

        deduplicatedStream.to(props.getKafkaTopicDeduplicatedOdeSpatJson(),
                Produced.with(Serdes.String(), JsonSerdes.OdeMessageFrame()));

        return builder.build();

    }

    public void stop() {
        logger.info("Stopping SPaT Deduplicator Socket Broadcast Topology.");
        if (streams != null) {
            streams.close();
            streams.cleanUp();
            streams = null;
        }
        logger.info("Stopped SPaT Deduplicator Socket Broadcast Topology.");
    }

    StateListener stateListener;

    public void registerStateListener(StateListener stateListener) {
        this.stateListener = stateListener;
    }

    StreamsUncaughtExceptionHandler exceptionHandler;

    public void registerUncaughtExceptionHandler(StreamsUncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }
}
