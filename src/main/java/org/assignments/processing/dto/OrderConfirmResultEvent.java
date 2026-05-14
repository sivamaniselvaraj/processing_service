package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

// Inbound: from Order service (confirm result)
@Data
@Builder
public class OrderConfirmResultEvent {
    private UUID jobId;
    private UUID orderId;
    private UUID customerId;
    private boolean success;
    private String failureReason;
}