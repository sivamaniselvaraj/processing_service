package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.UUID;

// Outbound: to Inventory service
@Data
@Builder
public class InventoryCheckEvent {
    private UUID jobId;
    private UUID orderId;
    private UUID customerId;
    private List<OrderItem> items;
}