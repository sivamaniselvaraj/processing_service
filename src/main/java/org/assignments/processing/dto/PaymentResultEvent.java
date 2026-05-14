package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

// Inbound: from Payment service
@Data
@Builder
public class PaymentResultEvent {
    private UUID jobId;
    private UUID orderId;
    private boolean success;
    private String transactionId;
    private String failureReason;
}