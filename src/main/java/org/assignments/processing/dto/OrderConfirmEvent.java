package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

// Outbound: to Order service (confirm)
@Data
@Builder
public class OrderConfirmEvent {
    private UUID jobId;
    private UUID orderId;
    private String transactionId;
}