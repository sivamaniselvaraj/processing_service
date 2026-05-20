package org.assignments.processing.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemInventoryResult {
    private String productCode;
    private String productName;
    private Integer requestedQuantity;
    private Integer availableQuantity;
    private boolean available;
    private String reason;
}
