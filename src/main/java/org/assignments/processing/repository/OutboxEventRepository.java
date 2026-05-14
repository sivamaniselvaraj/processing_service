package org.assignments.processing.repository;

import org.assignments.processing.entity.OutboxEvent;
import org.assignments.processing.enums.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    /** Fetch pending events ordered by creation — for the poller */
    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxStatus status);

    /** Full audit trail for a saga by correlation key */
    List<OutboxEvent> findByCorrelationIdOrderByCreatedAtAsc(UUID correlationId);

    /** All events for an order */
    List<OutboxEvent> findByOrderIdOrderByCreatedAtAsc(UUID orderId);

    /** Pending events that still have retries left */
    @Query("""
        SELECT o FROM OutboxEvent o
        WHERE o.status = 'PENDING'
          AND o.retryCount < o.maxRetries
        ORDER BY o.createdAt ASC
    """)
    List<OutboxEvent> findRetryableEvents();

    /** Check for duplicate event before inserting (idempotency) */
    boolean existsByCorrelationIdAndEventType(UUID correlationId, String eventType);
}
