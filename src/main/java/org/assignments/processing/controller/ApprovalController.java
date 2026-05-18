package org.assignments.processing.controller;


import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assignments.processing.dto.ApprovalRequest;
import org.assignments.processing.dto.ApprovalResponse;
import org.assignments.processing.service.ApprovalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/approvals")
//@RequiredArgsConstructor
public class ApprovalController {

    @Autowired
    private ApprovalService approvalService;

    /**
     * GET /api/v1/approvals/{jobId}
     * Fetch the current approval status of a job.
     */
    @GetMapping("/{jobId}")
    public ResponseEntity<ApprovalResponse> getApprovalStatus(@PathVariable UUID jobId) {
        log.info("GET approval status jobId={}", jobId);
        ApprovalResponse response = approvalService.getApprovalStatus(jobId);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/approvals/{jobId}/approve
     * Approve a job — advances the saga to the next step.
     *
     * Body: { "approvedBy": "admin@example.com", "remarks": "Looks good" }
     */
    @PostMapping("/{jobId}/approve")
    public ResponseEntity<ApprovalResponse> approve(
            @PathVariable UUID jobId,
            @Valid @RequestBody ApprovalRequest request) {

        log.info("APPROVE request for jobId={} by {}", jobId, request.getApprovedBy());
        ApprovalResponse response = approvalService.approve(jobId, request);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/approvals/{jobId}/reject
     * Reject a job — triggers saga compensation/rollback.
     *
     * Body: { "approvedBy": "admin@example.com", "remarks": "Invalid items" }
     */
    @PostMapping("/{jobId}/reject")
    public ResponseEntity<ApprovalResponse> reject(
            @PathVariable UUID jobId,
            @Valid @RequestBody ApprovalRequest request) {

        log.info("REJECT request for jobId={} by {}", jobId, request.getApprovedBy());
        ApprovalResponse response = approvalService.reject(jobId, request);
        return ResponseEntity.ok(response);
    }
}
