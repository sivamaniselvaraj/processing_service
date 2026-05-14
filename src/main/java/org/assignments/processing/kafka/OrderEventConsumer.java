package org.assignments.processing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.assignments.processing.dto.OrderEvent;
import org.assignments.processing.service.ProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class OrderEventConsumer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    ProcessingService processingService;


    //@KafkaListener(topics = "order_topic", groupId = "processing-group", containerFactory= "kafkaListenerContainerFactory")
    @KafkaListener(
            topics        = "${processing.kafka.consumer-topics.order-created}",
            groupId       = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String correlationId = extractHeader(record, "X-Correlation-Id");
        String eventType     = extractHeader(record, "X-Event-Type");

        log.info("ORDER_CREATED consumed topic={} partition={} offset={} correlationId={} eventType={}",
                record.topic(), record.partition(), record.offset(), correlationId, eventType);

        try {
            OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
            processingService.handleOrderCreated(event);
            ack.acknowledge();
            log.info("ORDER_CREATED processed and ACKed correlationId={}", correlationId);

        } catch (Exception ex) {
            log.error("Failed to process ORDER_CREATED correlationId={} error={}",
                    correlationId, ex.getMessage(), ex);
            // Do NOT ack — message will be retried by Kafka
            // After max retries, it lands in the Dead Letter Topic (DLT)
        }
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : "N/A";
    }
}
