package org.assignments.processing.enums;


/**
 * ApprovalStatus — tracks whether a saga step is awaiting
 * human approval, has been approved, or rejected.
 *
 * Used as a column on ProcessingStatus:
 *
 *   PENDING_APPROVAL  → saga is paused, waiting for a human decision
 *   APPROVED          → user approved; saga advances to next step
 *   REJECTED          → user rejected; saga triggers compensation
 *   NOT_REQUIRED      → this step does not need approval (default)
 */
public enum ApprovalStatus {
    NOT_REQUIRED,
    PENDING_APPROVAL,
    APPROVED,
    REJECTED
}

