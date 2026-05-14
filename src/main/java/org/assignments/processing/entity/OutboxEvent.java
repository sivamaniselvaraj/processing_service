package org.assignments.processing.entity;

import jakarta.persistence.*;
import lombok.*;
import org.assignments.processing.enums.OutboxStatus;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name="OutboxEvents")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "event_id", updatable = false, nullable = false)
    private UUID eventId;

    /**
     * Correlation Key — the saga's jobId.
     * Carried in every event header so any consumer can trace
     * the full saga chain from ORDER_CREATED → COMPLETED.
     */
    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    /**
     * The originating order — useful for querying all events for an order
     * without joining through jobs.
     */
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    /**
     * Kafka topic to publish to
     */
    @Column(name = "topic", nullable = false, length = 100)
    private String topic;

    /**
     * Event type — e.g. INVENTORY_CHECK_REQUESTED, PAYMENT_REQUESTED
     */
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    /**
     * Serialized JSON payload
     */
    @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    /**
     * Kafka partition key — ensures ordered delivery per saga
     */
    @Column(name = "partition_key", length = 100)
    private String partitionKey;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private int retryCount = 0;

    @Column(name = "max_retries", nullable = false)
    @Builder.Default
    private int maxRetries = 3;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        // Default partition key to correlationId for ordered per-saga delivery
        if (this.partitionKey == null) {
            this.partitionKey = this.correlationId.toString();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}