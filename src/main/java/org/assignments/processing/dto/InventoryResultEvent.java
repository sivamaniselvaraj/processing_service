package org.assignments.processing.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assignments.processing.enums.InventoryStatus;
import org.assignments.processing.enums.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

// Inbound: from Inventory service
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class InventoryResultEvent {
    private UUID correlationId;
    private UUID orderId;
    private UUID customerId;
    private OrderStatus orderStatus;
    private InventoryStatus inventoryStatus;
    private String message;
    private List<ItemInventoryResult> itemResults;
    @Builder.Default
    private String eventType = "INVENTORY_CHECK_RESULT";

    @Builder.Default
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime processedAt = LocalDateTime.now();
    private ItemInventoryResult itemInventoryResult;

    //"{\"correlationId\":\"6549bc93-c0ad-413e-be0d-9a4d8e85654f\",\"orderId\":\"a2df36af-a0fb-4fb3-b2dc-329429f841f4\",\"customerId\":\"b356d0a8-0004-431d-b069-a251a1c04abd\",\"orderStatus\":\"REJECTED\",\"inventoryStatus\":\"INSUFFICIENT_STOCK\",\"message\":\"One or more items are not available in sufficient quantity.\",\"itemResults\":[{\"productCode\":\"12345\",\"productName\":\"Sivaji Rice Ponni Boiled\",\"requestedQuantity\":1,\"availableQuantity\":null,\"available\":false,\"reason\":\"Insufficient or unavailable stock\"}],\"eventType\":\"INVENTORY_CHECK_RESULT\",\"processedAt\":\"2026-05-19 17:30:17\",\"itemInventoryResult\":null}"
}