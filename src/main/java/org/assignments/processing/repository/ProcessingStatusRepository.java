package org.assignments.processing.repository;

import jakarta.transaction.Transactional;
import org.assignments.processing.entity.ProcessingStatus;
import org.assignments.processing.enums.ApprovalStatus;
import org.assignments.processing.enums.SagaState;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
@Transactional
public interface ProcessingStatusRepository extends JpaRepository<ProcessingStatus, UUID> {

    Optional<ProcessingStatus> findByJobJobId(UUID jobId);

    List<ProcessingStatus> findBySagaState(SagaState sagaState);

    Optional<ProcessingStatus> findByOrderId(UUID orderId);


    List<ProcessingStatus> findByJobJobIdIn(Collection<UUID> jobIds);

    Page<ProcessingStatus> findByApprovalStatusAndSagaState(
            ApprovalStatus approvalStatus,
            SagaState sagaState,
            Pageable pageable);
}
