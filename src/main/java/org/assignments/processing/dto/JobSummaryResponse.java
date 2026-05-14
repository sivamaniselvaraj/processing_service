package org.assignments.processing.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Lightweight projection used in list/page responses.
 * Avoids exposing the full entity graph.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobSummaryResponse {

    private UUID          jobId;
    private UUID          orderId;
    private String        jobType;
    private String        jobStatus;
    private String        priority;
    private int           retryCount;
    private int           maxRetries;

    // Processing status fields
    private String        currentStep;
    private String        sagaState;
    private String        approvalStatus;
    private String        approvedBy;
    private String        approvalRemarks;
    private LocalDateTime approvedAt;
    private String        lastEvent;
    private String        errorMessage;

    // Timestamps
    private LocalDateTime scheduledAt;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
