package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

// Outbound: saga terminal events
@Data
@Builder
public class OrderProcessingCompletedEvent {
    private UUID jobId;
    private UUID orderId;
}
