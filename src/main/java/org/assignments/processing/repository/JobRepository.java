package org.assignments.processing.repository;

import jakarta.transaction.Transactional;
import org.assignments.processing.entity.Job;
import org.assignments.processing.enums.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
@Transactional
public interface JobRepository extends JpaRepository<Job, UUID> {
    boolean existsByOrderId(UUID orderId);

//    List<Job> findByJobStatusIn(List<String> status);
//
//    Optional<Job> findByJobIdAndStatus(UUID jobId, JobStatus status);

    Optional<Job> findByOrderId(UUID orderId);

    Page<Job> findAllByOrderId(UUID orderId, Pageable pageable);

    List<Job> findByStatus(String orderStatus);

    @Query("""
        SELECT j FROM Job j
        WHERE (:status IS NULL OR j.status = :status)
        ORDER BY j.createdAt DESC
    """)
    Page<Job> findWithFilters(@Param("status") JobStatus status, Pageable pageable);

    @Query("""
        SELECT j FROM Job j
        WHERE j.status IN ('FAILED', 'RETRYING')
          AND j.retryCount < j.maxRetries
    """)
    List<Job> findRetryableJobs();
}
