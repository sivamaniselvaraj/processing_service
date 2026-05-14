package org.assignments.processing.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderItem {
    private UUID id;
    private int quantity;
    private BigDecimal price;
    private BigDecimal subtotal;
    private String name;
}