package us.dot.its.jpo.deduplicator;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/**
 * Deserialization exception handler.
 * Adds more detailed logging to diagnose issues relative the
 * built in {@link org.apache.kafka.streams.errors.LogAndContinueExceptionHandler}
 */
@Slf4j
public class CustomDeserializationExceptionHandler implements
    DeserializationExceptionHandler {

  @Override
  public DeserializationHandlerResponse handle(final ErrorHandlerContext context,
      final ConsumerRecord<byte[], byte[]> record,
      final Exception exception) {
    try {
      String key = record.key() != null
          ? new String(record.key(), StandardCharsets.UTF_8)
          : "null";
      String value = record.value() != null
          ? new String(record.value(), StandardCharsets.UTF_8)
          : "null";
      log.error("Exception caught during Deserialization, " +
              "taskId: {}, timestamp: {}, topic: {}, partition: {}, offset: {}, key: {}, value: {}",
          context.taskId(), context.timestamp(),
          record.topic(), record.partition(), record.offset(),
          key, value,  exception);
    } catch (Exception e) {
      log.error("Error logging deserialization exception", e);
    }
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    // ignore
  }
}
