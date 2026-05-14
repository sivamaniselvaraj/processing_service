package org.assignments.processing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.assignments.processing.dto.*;
import org.assignments.processing.service.ProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * ProcessingResultConsumer — listens to all saga reply topics:
 *
 *  inventory.check.result      → INVENTORY_CHECK_RESULT
 *  payment.result              → PAYMENT_RESULT
 *  order.confirm.result        → ORDER_CONFIRM_RESULT
 *  notification.result         → NOTIFICATION_RESULT
 *  saga.compensation.result    → COMPENSATION_RESULT
 *
 * Each listener reads the X-Event-Type header to route to the
 * correct ProcessingService handler method.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcessingResultConsumer {
    @Autowired
    ProcessingService processingService;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // ─── Inventory Result ─────────────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.inventory-result}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onInventoryResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        log.info("INVENTORY_RESULT consumed correlationId={}", correlationId);

        try {
            InventoryResultEvent event = objectMapper.readValue(record.value(), InventoryResultEvent.class);
            processingService.handleInventoryCheckResult(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process INVENTORY_RESULT correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
        }
    }

    // ─── Payment Result ───────────────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.payment-result}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        log.info("PAYMENT_RESULT consumed correlationId={}", correlationId);

        try {
            PaymentResultEvent event = objectMapper.readValue(record.value(), PaymentResultEvent.class);
            processingService.handlePaymentResult(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process PAYMENT_RESULT correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
        }
    }

    // ─── Order Confirm Result ─────────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.order-confirm-result}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderConfirmResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        log.info("ORDER_CONFIRM_RESULT consumed correlationId={}", correlationId);

        try {
            OrderConfirmResultEvent event = objectMapper.readValue(record.value(), OrderConfirmResultEvent.class);
            processingService.handleOrderConfirmResult(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process ORDER_CONFIRM_RESULT correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
        }
    }

    // ─── Notification Result ──────────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.notification-result}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onNotificationResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        log.info("NOTIFICATION_RESULT consumed correlationId={}", correlationId);

        try {
            NotificationResultEvent event = objectMapper.readValue(record.value(), NotificationResultEvent.class);
            processingService.handleNotificationResult(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process NOTIFICATION_RESULT correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
        }
    }

    // ─── Compensation Result ──────────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.compensation-result}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onCompensationResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        log.info("COMPENSATION_RESULT consumed correlationId={}", correlationId);

        try {
            CompensationResultEvent event = objectMapper.readValue(record.value(), CompensationResultEvent.class);
            processingService.handleCompensationComplete(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process COMPENSATION_RESULT correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
        }
    }

    // ─── Dead Letter Topic handler ────────────────────────────

    @KafkaListener(
            topics           = "${processing.kafka.consumer-topics.dlt}",
            groupId          = "${spring.kafka.consumer.group-id}-dlt",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        String eventType     = extractHeader(record, "X-Event-Type");

        log.error("DLT message received topic={} correlationId={} eventType={} payload={}",
                record.topic(), correlationId, eventType, record.value());

        // Persist to a dead_letter_log table or alert monitoring
        ack.acknowledge();
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : "N/A";
    }
}