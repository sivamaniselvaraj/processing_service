package org.assignments.processing.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assignments.processing.dto.*;
import org.assignments.processing.entity.Job;
import org.assignments.processing.entity.ProcessingStatus;
import org.assignments.processing.enums.ApprovalStatus;
import org.assignments.processing.enums.JobStatus;
import org.assignments.processing.enums.SagaState;
import org.assignments.processing.enums.SagaStep;
import org.assignments.processing.exception.ApprovalException;
import org.assignments.processing.exception.JobNotFoundException;
import org.assignments.processing.exception.SagaException;
import org.assignments.processing.repository.JobRepository;
import org.assignments.processing.repository.ProcessingStatusRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * ApprovalService — handles human approval/rejection of saga steps.
 *
 * Flow:
 *  1. Saga reaches ORDER_CONFIRMED step → sets approvalStatus = PENDING_APPROVAL
 *     sagaState = AWAITING_APPROVAL (saga is paused)
 *  2. User calls POST /approve or /reject via ApprovalController
 *  3. On APPROVE → resume saga, publish next outbox event
 *  4. On REJECT  → trigger compensation rollback
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ApprovalService {

    @Autowired
     JobRepository jobRepository;
    @Autowired
    ProcessingStatusRepository processingStatusRepository;
    @Autowired
    OutboxService              outboxService;
    @Autowired
    ProcessingService          processingService;

    @Value("${processing.kafka.topics.payment.requested}")
    private static String TOPIC_PAYMENT        = "payment.requested";
    @Value("${processing.kafka.topics.order.confirm.requested}")
    private static final String TOPIC_ORDER_CONFIRM  = "order.confirm.requested";
    @Value("${processing.kafka.topics.notification.requested}")
    private static final String TOPIC_NOTIFICATION   = "notification.requested";
    @Value("${processing.kafka.topics.saga.compensation.requested}")
    private static final String TOPIC_COMPENSATION   = "saga.compensation.requested";
    @Value("${processing.kafka.topics.saga.result}")
    private static final String TOPIC_SAGA_RESULT    = "saga.result";

    // =========================================================
    // GET — current approval status
    // =========================================================

    @Transactional
    public ApprovalResponse getApprovalStatus(UUID jobId) {
        Job job = getJobOrThrow(jobId);
        ProcessingStatus ps = getProcessingStatusOrThrow(jobId);

        return buildResponse(job, ps, "Current approval status retrieved.");
    }

    // =========================================================
    // APPROVE — advance saga to next step
    // =========================================================

    @Transactional
    public ApprovalResponse approve(UUID jobId, ApprovalRequest request) {
        Job job = getJobOrThrow(jobId);
        ProcessingStatus ps = getProcessingStatusOrThrow(jobId);

        validateAwaitingApproval(ps, jobId, "approve");

        log.info("Approving jobId={} step={} by={}",
                jobId, ps.getCurrentStep(), request.getApprovedBy());

        // Record the decision on processing_status
        ps.recordApproval(request.getApprovedBy(), request.getRemarks(), ApprovalStatus.APPROVED);
        ps.markStepCompleted();
        ps.setSagaState(SagaState.IN_PROGRESS);
        processingStatusRepository.save(ps);

        // Resume saga — publish the next step's outbox event
        resumeSagaAfterApproval(job, ps);

        log.info("Saga APPROVED and resumed for jobId={} nextStep follows.", jobId);
        return buildResponse(job, ps, "Job approved. Saga resumed successfully.");
    }

    // =========================================================
    // REJECT — trigger compensation
    // =========================================================

    @Transactional
    public ApprovalResponse reject(UUID jobId, ApprovalRequest request) {
        Job job = getJobOrThrow(jobId);
        ProcessingStatus ps = getProcessingStatusOrThrow(jobId);

        validateAwaitingApproval(ps, jobId, "reject");

        log.warn("Rejecting jobId={} step={} by={} reason={}",
                jobId, ps.getCurrentStep(), request.getApprovedBy(), request.getRemarks());

        // Record rejection on processing_status
        ps.recordApproval(request.getApprovedBy(), request.getRemarks(), ApprovalStatus.REJECTED);
        ps.markStepFailed("Rejected by " + request.getApprovedBy()
                + ". Reason: " + request.getRemarks(), true);
        ps.setSagaState(SagaState.COMPENSATING);
        processingStatusRepository.save(ps);

        // Mark job as FAILED
        job.setStatus(JobStatus.FAILED);
        jobRepository.save(job);

        // Trigger saga compensation
        processingService.handleCompensation(jobId);

        return buildResponse(job, ps, "Job rejected. Compensation triggered.");
    }

    // =========================================================
    // RESUME SAGA — called after approval, decide next step
    // =========================================================

    /**
     * After approval, resume the saga from the step that was awaiting approval.
     * Typically ORDER_CONFIRMED is the approval gate — after approval,
     * trigger the NOTIFICATION step.
     */
    private void resumeSagaAfterApproval(Job job, ProcessingStatus ps) {
        UUID correlationId = job.getJobId();

        SagaStep approvedStep = ps.getCurrentStep();

        switch (approvedStep) {
            case ORDER_CONFIRMED -> {
                // Approved at order confirmation gate → notify customer
                ps.advanceTo(SagaStep.NOTIFICATION_TRIGGERED, "APPROVAL_GRANTED");
                processingStatusRepository.save(ps);

                outboxService.createEvent(
                        correlationId,
                        job.getOrderId(),
                        TOPIC_NOTIFICATION,
                        "NOTIFICATION_REQUESTED",
                        NotificationEvent.builder()
                                .jobId(correlationId)
                                .orderId(job.getOrderId())
                                .notificationType("ORDER_APPROVED")
                                .build()
                );
            }
            case PAYMENT_PROCESSING -> {
                // Approved at payment gate → confirm order
                ps.advanceTo(SagaStep.ORDER_CONFIRMED, "APPROVAL_GRANTED");
                processingStatusRepository.save(ps);

                outboxService.createEvent(
                        correlationId,
                        job.getOrderId(),
                        TOPIC_ORDER_CONFIRM,
                        "ORDER_CONFIRM_REQUESTED",
                        OrderConfirmEvent.builder()
                                .jobId(correlationId)
                                .orderId(job.getOrderId())
                                .build()
                );
            }
            default -> throw new ApprovalException(
                    "Unexpected approval at step=" + approvedStep + " for jobId=" + job.getJobId());
        }
    }

    // =========================================================
    // PAUSE SAGA FOR APPROVAL — called from ProcessingService
    // =========================================================

    /**
     * Called by ProcessingService when a step requires human approval
     * before proceeding. Sets approvalStatus = PENDING_APPROVAL and
     * sagaState = AWAITING_APPROVAL (saga pauses here).
     */
    @Transactional
    public void requestApproval(UUID jobId, SagaStep step) {
        Job job = getJobOrThrow(jobId);
        ProcessingStatus ps = getProcessingStatusOrThrow(jobId);

        log.info("Saga PAUSED awaiting approval jobId={} step={}", jobId, step);

        ps.pendingApproval();
        ps.setSagaState(SagaState.AWAITING_APPROVAL);
        ps.setLastEvent("APPROVAL_REQUESTED");
        processingStatusRepository.save(ps);

        // Optionally notify approver via outbox
        outboxService.createEvent(
                jobId,
                job.getOrderId(),
                TOPIC_NOTIFICATION,
                "APPROVAL_REQUESTED",
                ApprovalRequestedEvent.builder()
                        .jobId(jobId)
                        .orderId(job.getOrderId())
                        .step(step.name())
                        .build()
        );
    }

    // =========================================================
    // HELPERS
    // =========================================================

    /**
     * Validates the job is currently in AWAITING_APPROVAL state.
     * Throws ApprovalException if it is not in the correct state.
     */
    private void validateAwaitingApproval(ProcessingStatus ps, UUID jobId, String action) {
        if (ps.getApprovalStatus() != ApprovalStatus.PENDING_APPROVAL) {
            throw new ApprovalException(String.format(
                    "Cannot %s jobId=%s — current approvalStatus=%s (expected PENDING_APPROVAL)",
                    action, jobId, ps.getApprovalStatus()
            ));
        }
        if (ps.getSagaState() != SagaState.AWAITING_APPROVAL) {
            throw new ApprovalException(String.format(
                    "Cannot %s jobId=%s — sagaState=%s is not AWAITING_APPROVAL",
                    action, jobId, ps.getSagaState()
            ));
        }
    }

    private ApprovalResponse buildResponse(Job job, ProcessingStatus ps, String message) {
        return ApprovalResponse.builder()
                .jobId(job.getJobId())
                .orderId(job.getOrderId())
                .approvalStatus(ps.getApprovalStatus().name())
                .currentStep(ps.getCurrentStep().name())
                .sagaState(ps.getSagaState().name())
                .jobStatus(job.getStatus().name())
                .approvedBy(ps.getApprovedBy())
                .remarks(ps.getApprovalRemarks())
                .approvedAt(ps.getApprovedAt())
                .message(message)
                .build();
    }

    private Job getJobOrThrow(UUID jobId) {
        return jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    private ProcessingStatus getProcessingStatusOrThrow(UUID jobId) {
        return processingStatusRepository.findByJobJobId(jobId)
                .orElseThrow(() -> new SagaException("ProcessingStatus not found for jobId=" + jobId));
    }
}
