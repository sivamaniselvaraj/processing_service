package org.assignments.processing.entity;

import jakarta.persistence.*;
import lombok.*;
import org.assignments.processing.enums.ApprovalStatus;
import org.assignments.processing.enums.SagaState;
import org.assignments.processing.enums.SagaStep;
import org.assignments.processing.enums.StepStatus;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "processing_status")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessingStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "status_id", updatable = false, nullable = false)
    private UUID statusId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false)
    private Job job;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false, length = 50)
    private SagaStep currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private StepStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "saga_state", nullable = false, length = 30)
    private SagaState sagaState;

    @Column(name = "compensation_needed", nullable = false)
    @Builder.Default
    private boolean compensationNeeded = false;

    /**
     * The last event that triggered a step change.
     * e.g. "ORDER_CREATED", "PAYMENT_SUCCESS", "INVENTORY_FAILED"
     */
    @Column(name = "last_event", length = 100)
    private String lastEvent;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    // ── Approval Gate Fields ──────────────────────────────────

    @Enumerated(EnumType.STRING)
    @Column(name = "approval_status", nullable = false, length = 30)
    @Builder.Default
    private ApprovalStatus approvalStatus = ApprovalStatus.NOT_REQUIRED;

    @Column(name = "approved_by", length = 150)
    private String approvedBy;

    @Column(name = "approval_remarks", columnDefinition = "TEXT")
    private String approvalRemarks;

    @Column(name = "approved_at")
    private LocalDateTime approvedAt;

    // ── Timestamps ────────────────────────────────────────────

    @Column(name = "step_started_at")
    private LocalDateTime stepStartedAt;

    @Column(name = "step_completed_at")
    private LocalDateTime stepCompletedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        if (this.stepStartedAt == null) {
            this.stepStartedAt = LocalDateTime.now();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

// ─── Convenience method to advance the saga step ─────────

    public void advanceTo(SagaStep nextStep, String triggerEvent) {
        this.stepCompletedAt = LocalDateTime.now();
        this.currentStep = nextStep;
        this.stepStartedAt = LocalDateTime.now();
        this.stepCompletedAt = null;
        this.lastEvent = triggerEvent;
        this.status = StepStatus.IN_PROGRESS;
        this.approvalStatus  = ApprovalStatus.NOT_REQUIRED;
    }

    public void markStepFailed(String errorMessage, boolean requiresCompensation) {
        this.status = StepStatus.FAILED;
        this.errorMessage = errorMessage;
        this.compensationNeeded = requiresCompensation;
        this.sagaState = requiresCompensation ? SagaState.COMPENSATING : SagaState.FAILED;
        this.stepCompletedAt = LocalDateTime.now();
    }

    public void markStepCompleted() {
        this.status = StepStatus.COMPLETED;
        this.stepCompletedAt = LocalDateTime.now();
    }

    /** Pause the saga at this step — wait for human approval */
    public void pendingApproval() {
        this.approvalStatus = ApprovalStatus.PENDING_APPROVAL;
        this.status         = StepStatus.IN_PROGRESS;
        this.sagaState      = SagaState.IN_PROGRESS;
    }

    /** Record approval decision */
    public void recordApproval(String approvedBy, String remarks, ApprovalStatus decision) {
        this.approvalStatus   = decision;
        this.approvedBy       = approvedBy;
        this.approvalRemarks  = remarks;
        this.approvedAt       = LocalDateTime.now();
    }
}