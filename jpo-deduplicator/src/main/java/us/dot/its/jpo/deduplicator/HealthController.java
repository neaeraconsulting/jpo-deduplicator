package us.dot.its.jpo.deduplicator;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import us.dot.its.jpo.deduplicator.deduplicator.DeduplicatorServiceController;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RestController
@RequestMapping(path = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
public class HealthController {

    private final DeduplicatorServiceController mainController;

    public HealthController(DeduplicatorServiceController mainController) {
        this.mainController = mainController;
    }

    private final Set<KafkaStreams.State> okStates = Set.of(KafkaStreams.State.RUNNING,
            KafkaStreams.State.REBALANCING);

    @GetMapping(value = "/check")
    public HealthStatus health() {
        try {
            boolean healthy = true;
            var sb = new StringBuilder();
            for (var entry : listStreams().entrySet()) {
                String name = entry.getKey();
                StreamsInfo info = entry.getValue();
                if (!okStates.contains(info.getState())) {
                    healthy = false;
                    sb.append(String.format("Stream %s is not ok, state: %s.  ", name, info.getState()));
                }
            }
            if (healthy) {
                return new HealthStatus(true, "streams are ok");
            } else {
                return new HealthStatus(false, sb.toString());
            }
        } catch (Exception e) {
            return new HealthStatus(false, "Exception: " + e.getMessage());
        }
    }

    public record HealthStatus(boolean healthy, String message){}

    /**
     * Lists all Kafka Streams topologies and their states.
     *
     * @return response entity containing a map of stream names to stream info
     */
    @GetMapping(value = "/streams")
    public @ResponseBody StreamsInfoMap listStreams() {
        var streamsMap = getKafkaStreamsMap();
        String baseUrl = baseUrl();
        var result = new StreamsInfoMap();
        for (String name : streamsMap.keySet()) {
            var streamsInfo = new StreamsInfo();
            String url = String.format("%s/health/streams/%s", baseUrl, name);
            streamsInfo.setDetailsUrl(url);
            result.put(name, streamsInfo);

            KafkaStreams streams = streamsMap.get(name);
            if (streams == null) {
                continue;
            }
            var state = streams.state();
            streamsInfo.setState(state);

        }
        return result;
    }

    /**
     * Returns metrics for a named Kafka Streams instance.
     *
     * @param name the name of the streams instance
     * @return response entity containing a map of metrics grouped by metric group
     */
    @GetMapping(value = "/streams/{name}")
    public MetricsGroupMap namedStreams(@PathVariable String name) {

        Map<String, KafkaStreams> streamsMap = getKafkaStreamsMap();
        KafkaStreams streams = streamsMap.get(name);
        if (streams == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                "Unknown streams topology: " + name);
        }


        var metrics = streams.metrics();
        var result = new MetricsGroupMap();

        for (MetricName metricName : metrics.keySet()) {
            String groupName = metricName.group();
            MetricsGroup group = null;
            if (result.containsKey(groupName)) {
                group = result.get(groupName);
            } else {
                group = new MetricsGroup();
                result.put(groupName, group);
            }
            var metric = metrics.get(metricName);
            var metricValue = metric.metricValue();
            group.put(metricName.name(), metricValue);
        }
        return result;

    }

    @ExceptionHandler(Throwable.class)
    public ResponseEntity<String> handleRuntime(Throwable ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("error: " + ex.getMessage());
    }


    /**
     * Returns a map of Kafka Streams instances keyed by algorithm name.
     *
     * @return map of stream names to KafkaStreams instances
     */
    private Map<String, KafkaStreams> getKafkaStreamsMap() {
        return new TreeMap<>(mainController.getStreamsMap());
    }

    /** Map of stream names to StreamsInfo objects. */
    public static class StreamsInfoMap extends TreeMap<String, StreamsInfo> {}

    /**
     * Information about a Kafka Streams instance, including state and details URL.
     */
    @Getter
    @Setter
    public static class StreamsInfo {
        KafkaStreams.State state;
        String detailsUrl;
    }

    /**
     * Returns the base URL for the current servlet context.
     *
     * @return base URL as a string
     */
    private String baseUrl() {
        return ServletUriComponentsBuilder.fromCurrentContextPath().build().toUriString();
    }

    /** Map of metric group names to MetricsGroup objects. */
    public static class MetricsGroupMap extends TreeMap<String, MetricsGroup> { }

    /** Map of metric names to metric values for a given group. */
    public static class MetricsGroup extends TreeMap<String, Object> { }

}
