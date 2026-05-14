package org.assignments.processing.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assignments.processing.dto.JobSummaryResponse;
import org.assignments.processing.entity.Job;
import org.assignments.processing.entity.ProcessingStatus;
import org.assignments.processing.enums.ApprovalStatus;
import org.assignments.processing.enums.JobStatus;
import org.assignments.processing.enums.SagaState;
import org.assignments.processing.exception.JobNotFoundException;
import org.assignments.processing.repository.JobRepository;
import org.assignments.processing.repository.ProcessingStatusRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class JobQueryService {
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private ProcessingStatusRepository processingStatusRepository;

    // ─── List jobs with optional filters ─────────────────────

    @Transactional
    public Page<JobSummaryResponse> findJobs(String jobStatus,
                                             String sagaState,
                                             String approvalStatus,
                                             Pageable pageable) {

        JobStatus parsedJobStatus      = parseEnum(jobStatus,      JobStatus.class);
        SagaState parsedSagaState      = parseEnum(sagaState,      SagaState.class);
        ApprovalStatus parsedApprovalStatus = parseEnum(approvalStatus, ApprovalStatus.class);

        Page<Job> jobPage = jobRepository.findWithFilters(
                parsedJobStatus, pageable);

        return enrichWithProcessingStatus(jobPage, parsedSagaState, parsedApprovalStatus, pageable);
    }

    // ─── Shortcut: pending approval ──────────────────────────

    @Transactional
    public Page<JobSummaryResponse> findPendingApproval(Pageable pageable) {
        Page<ProcessingStatus> psPage = processingStatusRepository
                .findByApprovalStatusAndSagaState(
                        ApprovalStatus.PENDING_APPROVAL,
                        SagaState.AWAITING_APPROVAL,
                        pageable);

        List<JobSummaryResponse> items = psPage.getContent()
                .stream()
                .map(ps -> toSummary(ps.getJob(), ps))
                .collect(Collectors.toList());

        return new PageImpl<>(items, pageable, psPage.getTotalElements());
    }

    // ─── Jobs by orderId ─────────────────────────────────────

    @Transactional
    public Page<JobSummaryResponse> findByOrderId(UUID orderId, Pageable pageable) {
        Page<Job> jobPage = jobRepository.findAllByOrderId(orderId, pageable);
        return enrichWithProcessingStatus(jobPage, null, null, pageable);
    }

    // ─── Helpers ─────────────────────────────────────────────

    /**
     * Joins the job page with their processing_status rows and builds DTOs.
     * Optionally filters by sagaState and approvalStatus post-join.
     */
    private Page<JobSummaryResponse> enrichWithProcessingStatus(
            Page<Job> jobPage,
            SagaState sagaStateFilter,
            ApprovalStatus approvalStatusFilter,
            Pageable pageable) {

        List<UUID> jobIds = jobPage.getContent()
                .stream()
                .map(Job::getJobId)
                .collect(Collectors.toList());

        Map<UUID, ProcessingStatus> psMap = processingStatusRepository
                .findByJobJobIdIn(jobIds)
                .stream()
                .collect(Collectors.toMap(ps -> ps.getJob().getJobId(), ps -> ps));

        List<JobSummaryResponse> items = jobPage.getContent()
                .stream()
                .filter(job -> {
                    ProcessingStatus ps = psMap.get(job.getJobId());
                    if (ps == null) return true;
                    if (sagaStateFilter != null && ps.getSagaState() != sagaStateFilter) return false;
                    if (approvalStatusFilter != null && ps.getApprovalStatus() != approvalStatusFilter) return false;
                    return true;
                })
                .map(job -> toSummary(job, psMap.get(job.getJobId())))
                .collect(Collectors.toList());

        return new PageImpl<>(items, pageable, jobPage.getTotalElements());
    }

    private JobSummaryResponse toSummary(Job job, ProcessingStatus ps) {
        JobSummaryResponse.JobSummaryResponseBuilder b = JobSummaryResponse.builder()
                .jobId(job.getJobId())
                .orderId(job.getOrderId())
                .jobType(job.getJobType().name())
                .jobStatus(job.getStatus().name())
                //.priority(job.getPriority().name())
                .retryCount(job.getRetryCount())
                .maxRetries(job.getMaxRetries())
                .scheduledAt(job.getScheduledAt())
                .startedAt(job.getStartedAt())
                .completedAt(job.getCompletedAt())
                .createdAt(job.getCreatedAt())
                .updatedAt(job.getUpdatedAt());

        if (ps != null) {
            b.currentStep(ps.getCurrentStep().name())
                    .sagaState(ps.getSagaState().name())
                    .approvalStatus(ps.getApprovalStatus().name())
                    .approvedBy(ps.getApprovedBy())
                    .approvalRemarks(ps.getApprovalRemarks())
                    .approvedAt(ps.getApprovedAt())
                    .lastEvent(ps.getLastEvent())
                    .errorMessage(ps.getErrorMessage());
        }

        return b.build();
    }

    public Job getJobOrThrow(UUID jobId) {
        return jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    private <E extends Enum<E>> E parseEnum(String value, Class<E> enumClass) {
        if (value == null || value.isBlank()) return null;
        try {
            return Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid enum value '{}' for {}", value, enumClass.getSimpleName());
            return null;
        }
    }
}
