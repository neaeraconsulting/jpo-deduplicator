package us.dot.its.jpo.deduplicator.utils;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

@Slf4j
public class OdeJsonUtils {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    public static Instant getOdeMessageFrameMessageTime(OdeMessageFrameData message) {
        try {
            if (message == null || message.getMetadata() == null) {
                log.warn("The MessageFrame message or metadata is null, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            String time = ((OdeMessageFrameMetadata) message.getMetadata()).getOdeReceivedAt();
            if (time == null || time.isEmpty()) {
                log.warn("The MessageFrame message has null or empty odeReceivedAt time, using epoch time");
                return Instant.ofEpochMilli(0);
            }

            return Instant.from(formatter.parse(time));
        } catch (Exception e) {
            log.warn("Failed to Parse Time: " + (message != null && message.getMetadata() != null
                    ? ((OdeMessageFrameMetadata) message.getMetadata()).getOdeReceivedAt()
                    : "null"), e);
            return Instant.ofEpochMilli(0);
        }
    }

    public static TemporaryID getBsmTemporaryID(OdeMessageFrameData frameData) {
        if (frameData == null) {
            throw new IllegalArgumentException("frameData is null");
        }
        if (frameData.getPayload() == null) {
            throw new IllegalArgumentException("frameData.payload is null");
        }
        var mf = frameData.getPayload().getData();
        if (!(mf instanceof BasicSafetyMessageMessageFrame bsmMf)) {
            throw new IllegalArgumentException(String.format(
                "Frame data %S is not of type BasicSafetyMessageMessageFrame.", mf));
        }
        if (bsmMf.getValue() == null) {
            throw new IllegalArgumentException("frameData.payload.value is null");
        }
        if (bsmMf.getValue().getCoreData() == null) {
            throw new IllegalArgumentException("frameData.payload.value.coreData is null");
        }
        TemporaryID id = bsmMf.getValue().getCoreData().getId();
        if (id == null) {
            throw new IllegalArgumentException("frameData.payload.value.coreData.id is null");
        }
        return id;
    }
}
