package org.assignments.processing.kafka.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assignments.processing.dto.NotificationEvent;
import org.assignments.processing.service.OutboxService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * NotificationPublisher — builds and enqueues notification events
 * into the outbox for reliable delivery to the Notification service.
 *
 * All notification types are routed through here to keep
 * the notification topic and payload structure in one place.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationPublisher {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private final OutboxService outboxService;

    @Value("${processing.kafka.topics.notification}")
    private String notificationTopic;


    // ─── Notification Types ───────────────────────────────────

    public void orderConfirmed(UUID correlationId, UUID orderId, UUID customerId) {
        publish(correlationId, orderId, NotificationEvent.builder()
                .jobId(correlationId)
                .orderId(orderId)
                .customerId(customerId)
                .notificationType("ORDER_CONFIRMED")
                .message("Your order has been confirmed and is being processed.")
                .build());
    }

    public void orderApproved(UUID correlationId, UUID orderId, UUID customerId) {
        publish(correlationId, orderId, NotificationEvent.builder()
                .jobId(correlationId)
                .orderId(orderId)
                .customerId(customerId)
                .notificationType("ORDER_APPROVED")
                .message("Your order has been approved.")
                .build());
    }

    public void orderFailed(UUID correlationId, UUID orderId, UUID customerId, String reason) {
        publish(correlationId, orderId, NotificationEvent.builder()
                .jobId(correlationId)
                .orderId(orderId)
                .customerId(customerId)
                .notificationType("ORDER_FAILED")
                .message("We're sorry, your order could not be processed. Reason: " + reason)
                .build());
    }

    public void orderCancelled(UUID correlationId, UUID orderId, UUID customerId) {
        publish(correlationId, orderId, NotificationEvent.builder()
                .jobId(correlationId)
                .orderId(orderId)
                .customerId(customerId)
                .notificationType("ORDER_CANCELLED")
                .message("Your order has been cancelled and any payments refunded.")
                .build());
    }

    public void approvalRequested(UUID correlationId, UUID orderId, String step) {
        publish(correlationId, orderId, NotificationEvent.builder()
                .jobId(correlationId)
                .orderId(orderId)
                .notificationType("APPROVAL_REQUESTED")
                .message("Order requires manual approval at step: " + step)
                .build());
    }

    // ─── Internal ─────────────────────────────────────────────

    private void publish(UUID correlationId, UUID orderId, NotificationEvent event) {
        log.info("Queuing notification correlationId={} type={}", correlationId, event.getNotificationType());
        outboxService.createEvent(
                correlationId,
                orderId,
                notificationTopic,
                event.getNotificationType(),
                event
        );
    }

}