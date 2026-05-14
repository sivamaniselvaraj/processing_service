package org.assignments.processing.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.*;

/**
 * Request body for approve/reject endpoints.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApprovalRequest {

    @NotBlank(message = "approvedBy is required")
    private String approvedBy;

    private String remarks;
}