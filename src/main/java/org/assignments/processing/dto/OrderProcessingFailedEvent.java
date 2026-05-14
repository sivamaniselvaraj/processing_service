package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class OrderProcessingFailedEvent {
    private UUID jobId;
    private UUID orderId;
    private String reason;
}
