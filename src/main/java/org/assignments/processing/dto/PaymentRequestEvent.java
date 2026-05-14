package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

// Outbound: to Payment service
@Data
@Builder
public class PaymentRequestEvent {
    private UUID jobId;
    private UUID orderId;
    private BigDecimal amount;
    private String currency;
}