package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

// Compensation events
@Data
@Builder
public class CompensationEvent {
    private UUID jobId;
    private UUID orderId;
    private String reason;
}