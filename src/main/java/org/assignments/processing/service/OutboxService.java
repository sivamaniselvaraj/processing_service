package org.assignments.processing.service;

import lombok.extern.slf4j.Slf4j;
import org.assignments.processing.entity.OutboxEvent;
import org.assignments.processing.repository.OutboxEventRepository;
import org.assignments.processing.utils.JSONOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class OutboxService {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    /**
     * Persist an outgoing event to the outbox.
     * Must be called within an active @Transactional context.
     *
     * @param correlationId  saga's jobId — the correlation key
     * @param orderId        originating order
     * @param topic          Kafka topic
     * @param eventType      event name e.g. INVENTORY_CHECK_REQUESTED
     * @param payload        event payload object (will be serialized to JSON)
     */
    public OutboxEvent createEvent(UUID correlationId,
                            UUID orderId,
                            String topic,
                            String eventType,
                            Object payload) {

        // Idempotency guard — don't insert duplicate events for the same saga step
        if (outboxEventRepository.existsByCorrelationIdAndEventType(correlationId, eventType)) {
            log.warn("Duplicate outbox event skipped: correlationId={} eventType={}", correlationId, eventType);
            return null;
        }

        String serializedPayload = JSONOperation.serialize(payload);

        OutboxEvent event = OutboxEvent.builder()
                .correlationId(correlationId)   // ← Correlation Key
                .orderId(orderId)
                .topic(topic)
                .eventType(eventType)
                .payload(serializedPayload)
                .partitionKey(correlationId.toString()) // order per-saga delivery on Kafka
                .build();

        OutboxEvent saved = outboxEventRepository.save(event);
        log.debug("Outbox event saved: eventId={} correlationId={} eventType={}",
                saved.getEventId(), correlationId, eventType);
        return saved;
    }

}
