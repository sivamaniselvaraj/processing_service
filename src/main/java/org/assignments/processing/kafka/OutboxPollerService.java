package org.assignments.processing.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assignments.processing.dto.InventoryCheckEvent;
import org.assignments.processing.entity.OutboxEvent;
import org.assignments.processing.enums.OutboxStatus;
import org.assignments.processing.repository.OutboxEventRepository;
import org.assignments.processing.utils.ApplicationConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * OutboxPoller — relays PENDING outbox events to Kafka.
 * Poll delay is read from application.yaml:
 *   processing.outbox.poller.fixed-delay-ms
 */
@Slf4j
@Service
//@RequiredArgsConstructor
public class OutboxPollerService {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;//

    @Value("${processing.outbox.retry.max-retries}")
    int maxRetries;   // ← from yaml

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * fixedDelayString reads processing.outbox.poller.fixed-delay-ms from yaml.
     * SpEL expression converts the long to a String as required by @Scheduled.
     */
    @Scheduled(fixedDelayString = "${processing.outbox.poller.fixed-delay-ms:2000}")
    @Transactional //("kafkaTransactionManager")//(transactionManager = "kafkaTransactionManager")
    //@Qualifier("kafkaTransactionManager")
    //kafkaTransactionManager,transactionManager
    public void pollAndPublish() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findRetryableEvents();
        if (pendingEvents.isEmpty()) return;

        log.info("OutboxPoller: processing {} pending event(s)", pendingEvents.size());
        pendingEvents.forEach(this::publishEvent);
    }

    private void publishEvent(OutboxEvent event) {
        try {
            log.info("publishEvent: correlationId={} type={} order_id={} topic={} payload: {}", event.getCorrelationId(),
                    event.getEventType(),event.getOrderId(), event.getTopic(), event.getPayload());


            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(buildProducerRecord(event));
            //kafkaTemplate.executeInTransaction(kt -> kt.send(buildProducerMessage(event)));

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Notification published: correlationId={} type={} topic={} partition={} offset={}",
                            event.getCorrelationId(),
                            event.getEventType(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());

                    event.setStatus(OutboxStatus.PUBLISHED);
                    event.setPublishedAt(LocalDateTime.now());
                    event.setErrorMessage(null);
                    outboxEventRepository.save(event);

                    log.info("Outbox published: eventId={} correlationId={} topic={} eventType={}",
                            event.getEventId(), event.getCorrelationId(),
                            event.getTopic(), event.getEventType());
                } else {
                    log.error("Notification FAILED to publish: correlationId={} type={} error={}",
                            event.getCorrelationId(), event.getEventType(), ex.getMessage(), ex);
                }
            });




        } catch (Exception ex) {
            int newRetryCount = event.getRetryCount() + 1;
            event.setRetryCount(newRetryCount);
            event.setErrorMessage(ex.getMessage());

            if (newRetryCount >= maxRetries) {
                event.setStatus(OutboxStatus.FAILED);
                log.error("Outbox FAILED after {}/{} retries: eventId={} correlationId={} error={}",
                        newRetryCount, maxRetries,
                        event.getEventId(), event.getCorrelationId(), ex.getMessage());
            } else {
                log.warn("Outbox publish failed (attempt {}/{}): eventId={} error={}",
                        newRetryCount, maxRetries, event.getEventId(), ex.getMessage());
            }
           log.info("Error while publishing ", ex);
            outboxEventRepository.save(event);
        }
    }

    private ProducerRecord<String, String> buildProducerRecord(OutboxEvent event) throws JsonProcessingException {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(),
                null,
                event.getPartitionKey(),
                event.getPayload()
        );

        record.headers().add(new RecordHeader(ApplicationConstants.HEADER_CORRELATION_ID,
                event.getCorrelationId().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(ApplicationConstants.HEADER_EVENT_TYPE,
                event.getEventType().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(ApplicationConstants.HEADER_ORDER_ID,
                event.getOrderId().toString().getBytes(StandardCharsets.UTF_8)));

        return record;
    }
}
