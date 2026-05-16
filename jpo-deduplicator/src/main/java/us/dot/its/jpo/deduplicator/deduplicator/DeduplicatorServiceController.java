package us.dot.its.jpo.deduplicator.deduplicator;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Controller;

import lombok.Getter;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.BsmDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.MapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.TimDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedBsmDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedMapWktDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.ProcessedSpatDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.filters.bsm.whitelist.BsmWhitelistProperties;
import us.dot.its.jpo.deduplicator.filters.bsm.whitelist.BsmWhitelistTopology;

@Controller
@DependsOn("createKafkaTopics")
@Profile("!test && !testConfig")
public class DeduplicatorServiceController {

    private static final Logger logger = LoggerFactory.getLogger(DeduplicatorServiceController.class);

    @Getter
    final ConcurrentHashMap<String, KafkaStreams> streamsMap = new ConcurrentHashMap<String, KafkaStreams>();

    @Autowired
    public DeduplicatorServiceController(final DeduplicatorProperties props,
            final BsmWhitelistProperties bsmWhitelistProps) {

        try {

            if (props.isEnableProcessedMapDeduplication()) {
                logger.info("Starting Processed Map Deduplicator");
                ProcessedMapDeduplicatorTopology processedMapDeduplicatorTopology = new ProcessedMapDeduplicatorTopology(props);
                processedMapDeduplicatorTopology.start();
                streamsMap.put("ProcessedMapDeduplicator", processedMapDeduplicatorTopology.getStreams());
            }

            if (props.isEnableProcessedMapWktDeduplication()) {
                logger.info("Starting Processed Map WKT Deduplicator");
                ProcessedMapWktDeduplicatorTopology processedMapWktDeduplicatorTopology = new ProcessedMapWktDeduplicatorTopology(props);
                processedMapWktDeduplicatorTopology.start();
                streamsMap.put("ProcessedMapWKTDeduplicator", processedMapWktDeduplicatorTopology.getStreams());
            }

            if (props.isEnableOdeMapDeduplication()) {
                logger.info("Starting Map Deduplicator");
                MapDeduplicatorTopology mapDeduplicatorTopology = new MapDeduplicatorTopology(props);
                mapDeduplicatorTopology.start();
                streamsMap.put("MapDeduplicator", mapDeduplicatorTopology.getStreams());
            }

            if (props.isEnableOdeTimDeduplication()) {
                logger.info("Starting TIM Deduplicator");
                TimDeduplicatorTopology timDeduplicatorTopology = new TimDeduplicatorTopology(props);
                timDeduplicatorTopology.start();
                streamsMap.put("TimDeduplicator", timDeduplicatorTopology.getStreams());
            }

            if (props.isEnableProcessedSpatDeduplication()) {
                logger.info("Starting Processed Spat Deduplicator");
                ProcessedSpatDeduplicatorTopology processedSpatDeduplicatorTopology = new ProcessedSpatDeduplicatorTopology(
                        props);
                processedSpatDeduplicatorTopology.start();
                streamsMap.put("ProcessedSpatDeduplicator", processedSpatDeduplicatorTopology.getStreams());
            }

            if (props.isEnableOdeBsmDeduplication()) {
                logger.info("Starting BSM Deduplicator");
                BsmDeduplicatorTopology bsmDeduplicatorTopology = new BsmDeduplicatorTopology(props);
                bsmDeduplicatorTopology.start();
                streamsMap.put("BsmDeduplicator", bsmDeduplicatorTopology.getStreams());
            }

            if (props.isEnableProcessedBsmDeduplication()) {
                logger.info("Starting Processed BSM Deduplicator");
                ProcessedBsmDeduplicatorTopology processedBsmDeduplicatorTopology = new ProcessedBsmDeduplicatorTopology(
                        props);
                processedBsmDeduplicatorTopology.start();
                streamsMap.put("ProcessedBsmDeduplicator", processedBsmDeduplicatorTopology.getStreams());
            }

            if (bsmWhitelistProps.isEnabled()) {
                logger.info("Starting BSM Whitelist topology");
                var bsmWhitelistTopology = new BsmWhitelistTopology(props, bsmWhitelistProps);
                bsmWhitelistTopology.start();
                streamsMap.put(BsmWhitelistTopology.TOPOLOGY_NAME, bsmWhitelistTopology.getStreams());
            }


        } catch (Exception e) {
            logger.error("Encountered issue with creating topologies", e);
        }
    }
}
