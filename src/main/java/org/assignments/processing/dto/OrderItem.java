package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
public class OrderItem {
    private UUID productId;
    private int quantity;
    private BigDecimal unitPrice;
}