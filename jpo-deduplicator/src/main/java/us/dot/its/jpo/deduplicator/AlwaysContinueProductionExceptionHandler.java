package us.dot.its.jpo.deduplicator;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

@Slf4j
public class AlwaysContinueProductionExceptionHandler implements ProductionExceptionHandler {
    @Override
    public void configure(Map<String, ?> configs) {
        // Nothing to configure
    }

    @Override
    public ProductionExceptionHandlerResponse handle(ErrorHandlerContext context,
        ProducerRecord<byte[], byte[]> record, Exception exception) {
        try {
            String key = record.key() != null
                ? new String(record.key(), StandardCharsets.UTF_8)
                : "null";
            String value = record.value() != null
                ? new String(record.value(), StandardCharsets.UTF_8)
                : "null";
            log.error("Exception caught during production, " +
                    "taskId: {}, timestamp: {}, offset: {}, topic: {}, partition: {}, key: {},"
                    + " value: {}",
                context.taskId(), context.timestamp(), context.offset(),
                record.topic(), record.partition(), key, value,
                exception);
        } catch (Exception e) {
            log.error("Exception logging production exception", e);
        }
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(
        ErrorHandlerContext context, ProducerRecord record, Exception exception,
        SerializationExceptionOrigin origin) {
        try {
            log.error("Exception caught during Deserialization, " +
                    "taskId: {}, topic: {}, timestamp: {}, offset: {}, partition: {}, key: {}, "
                    + "value: {}, origin is key or value: {}",
                context.taskId(), record.topic(), context.timestamp(), context.offset(),
                record.partition(), record.key(), record.value(),
                origin, exception);
        } catch (Exception e) {
            log.error("Exception logging production serialization exception", e);
        }
        return ProductionExceptionHandlerResponse.CONTINUE;
    }
}