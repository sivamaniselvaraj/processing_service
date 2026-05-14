package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

// Inbound: from Inventory service
@Data
@Builder
public class InventoryResultEvent {
    private UUID jobId;
    private UUID orderId;
    private boolean success;
    private BigDecimal totalAmount;
    private String currency;
    private String failureReason;
}