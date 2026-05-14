package org.assignments.processing.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Response returned from all approval endpoints.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApprovalResponse {

    private UUID        jobId;
    private UUID        orderId;
    private String      approvalStatus;    // PENDING_APPROVAL | APPROVED | REJECTED
    private String      currentStep;       // current saga step
    private String      sagaState;         // overall saga state
    private String      jobStatus;         // PENDING | IN_PROGRESS | COMPLETED | FAILED
    private String      approvedBy;
    private String      remarks;
    private LocalDateTime approvedAt;
    private String      message;           // human-readable summary
}