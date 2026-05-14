package org.assignments.processing.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assignments.processing.dto.ApprovalRequest;
import org.assignments.processing.dto.ApprovalResponse;
import org.assignments.processing.dto.JobSummaryResponse;
import org.assignments.processing.service.ApprovalService;
import org.assignments.processing.service.JobQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * ProcessingController — REST API for:
 *  1. Querying jobs/orders by status (with pagination)
 *  2. Manual approval / rejection of saga steps
 */
@Slf4j
@RestController
@RequestMapping("/processing")
@RequiredArgsConstructor
public class ProcessingController {
    @Autowired
    private ApprovalService approvalService;

    @Autowired
    private JobQueryService jobQueryService;

    // =========================================================
    // LIST / QUERY ENDPOINTS
    // =========================================================

    /**
     * GET /api/v1/processing/jobs
     * List all jobs with optional filters.
     *
     * Query params:
     *   jobStatus      - PENDING | IN_PROGRESS | COMPLETED | FAILED | RETRYING | CANCELLED
     *   sagaState      - STARTED | IN_PROGRESS | AWAITING_APPROVAL | COMPLETED | COMPENSATING | COMPENSATED | FAILED
     *   approvalStatus - PENDING_APPROVAL | APPROVED | REJECTED | NOT_REQUIRED
     *   page, size, sort (Spring Pageable)
     *
     * Example:
     *   GET /api/v1/processing/jobs?jobStatus=IN_PROGRESS&page=0&size=20
     */
    @GetMapping("/jobs")
    public ResponseEntity<Page<JobSummaryResponse>> listJobs(
            @RequestParam(required = false) String jobStatus,
            @RequestParam(required = false) String sagaState,
            @RequestParam(required = false) String approvalStatus,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        log.info("GET /jobs jobStatus={} sagaState={} approvalStatus={}",
                jobStatus, sagaState, approvalStatus);

        Page<JobSummaryResponse> page = jobQueryService.findJobs(
                jobStatus, sagaState, approvalStatus, pageable);
        return ResponseEntity.ok(page);
    }

    /**
     * GET /api/v1/processing/jobs/pending-approval
     * Shortcut — fetch all jobs currently awaiting human approval.
     * Used by the admin dashboard.
     */
    @GetMapping("/jobs/pending-approval")
    public ResponseEntity<Page<JobSummaryResponse>> listPendingApproval(
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        log.info("GET /jobs/pending-approval");
        Page<JobSummaryResponse> page = jobQueryService.findPendingApproval(pageable);
        return ResponseEntity.ok(page);
    }

    /**
     * GET /api/v1/processing/jobs/{jobId}
     * Fetch full details of a single job including approval state.
     */
    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<ApprovalResponse> getJob(@PathVariable UUID jobId) {
        log.info("GET /jobs/{}", jobId);
        return ResponseEntity.ok(approvalService.getApprovalStatus(jobId));
    }

    /**
     * GET /api/v1/processing/orders/{orderId}/jobs
     * All jobs tied to a given orderId (across retries, cancellations etc.)
     */
    @GetMapping("/orders/{orderId}/jobs")
    public ResponseEntity<Page<JobSummaryResponse>> listJobsByOrder(
            @PathVariable UUID orderId,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        log.info("GET /orders/{}/jobs", orderId);
        Page<JobSummaryResponse> page = jobQueryService.findByOrderId(orderId, pageable);
        return ResponseEntity.ok(page);
    }

    // =========================================================
    // APPROVAL ENDPOINTS
    // =========================================================

    /**
     * POST /api/v1/processing/jobs/{jobId}/approve
     * Approve a paused job — resumes the saga.
     */
    @PostMapping("/jobs/{jobId}/approve")
    public ResponseEntity<ApprovalResponse> approve(
            @PathVariable UUID jobId,
            @Valid @RequestBody ApprovalRequest request) {

        log.info("POST /jobs/{}/approve by={}", jobId, request.getApprovedBy());
        return ResponseEntity.ok(approvalService.approve(jobId, request));
    }

    /**
     * POST /api/v1/processing/jobs/{jobId}/reject
     * Reject a paused job — triggers compensation rollback.
     */
    @PostMapping("/jobs/{jobId}/reject")
    public ResponseEntity<ApprovalResponse> reject(
            @PathVariable UUID jobId,
            @Valid @RequestBody ApprovalRequest request) {

        log.info("POST /jobs/{}/reject by={}", jobId, request.getApprovedBy());
        return ResponseEntity.ok(approvalService.reject(jobId, request));
    }
}