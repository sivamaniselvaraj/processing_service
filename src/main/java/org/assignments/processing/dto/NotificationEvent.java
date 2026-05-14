package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
public class NotificationEvent {
    /** Correlation Key — ties this notification to its saga instance */
    private UUID   jobId;

    private UUID   orderId;

    /** Customer to notify — null for internal/admin notifications */
    private UUID   customerId;

    /**
     * Notification type — used by the Notification service to pick
     * the correct template and channel.
     *
     * Values: ORDER_CONFIRMED | ORDER_APPROVED | ORDER_FAILED |
     *         ORDER_CANCELLED | APPROVAL_REQUESTED
     */
    private String notificationType;

    /** Human-readable message body */
    private String message;
}

