package org.assignments.processing.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class CompensationResultEvent {
    private UUID jobId;
    private boolean success;
}
