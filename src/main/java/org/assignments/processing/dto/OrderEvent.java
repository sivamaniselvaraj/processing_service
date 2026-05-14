package org.assignments.processing.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder

// Inbound: from Order service
public class OrderEvent {
    private UUID orderId;
    private UUID customerId;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    private String currency;
    private boolean express;
}