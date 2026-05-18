package org.assignments.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.assignments.processing.dto.*;
import org.assignments.processing.entity.Job;
import org.assignments.processing.entity.ProcessingStatus;
import org.assignments.processing.enums.*;
import org.assignments.processing.exception.JobNotFoundException;
import org.assignments.processing.exception.SagaException;
import org.assignments.processing.repository.JobRepository;
import org.assignments.processing.repository.ProcessingStatusRepository;
import org.assignments.processing.utils.JSONOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
@Slf4j
//@AllArgsConstructor
//@RequiredArgsConstructor
//@ConfigurationProperties(prefix = "processing")
public class ProcessingService {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ProcessingStatusRepository processingStatusRepository;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private JobQueryService jobQueryService;



    // Kafka topics
    @Value("${processing.kafka.topics.inventory-requested:inventory.check.requested}")
    private String TOPIC_INVENTORY;

    @Value("${processing.kafka.topics.payment-requested:payment.requested}")
    private String TOPIC_PAYMENT;
    @Value("${processing.kafka.topics.order.confirm-requested:order.confirm.requested}")
    private String TOPIC_ORDER_CONFIRM;
    @Value("${processing.kafka.topics.notification-requested:notification.requested}")
    private String TOPIC_NOTIFICATION;
    @Value("${processing.kafka.topics.saga.compensation-requested:saga.compensation.requested}")
    private String TOPIC_COMPENSATION;
    @Value("${processing.kafka.topics.saga.result:saga.result}")
    private String TOPIC_SAGA_RESULT;

    @Value("${processing.job.retry.max-retries:3}")
    private int MAX_RETRIES;

    @Value("${processing.saga.notification-critical:false}")
    private boolean NOTIFICATION_CRITICAL;


    // =========================================================
    // 1. ORDER RECEIVED
    // =========================================================

    //@Transactional
    @Transactional("transactionManager")
    public Job handleOrderCreated(OrderEvent orderEvent) {
        log.info("ORDER_CREATED received orderId={}", orderEvent.getOrderId());

        // Idempotency: skip if already processed
        if (jobRepository.existsByOrderId(orderEvent.getOrderId())) {
            log.warn("Duplicate ORDER_CREATED for orderId={}, skipping.", orderEvent.getOrderId());
            return jobRepository.findByOrderId(orderEvent.getOrderId())
                    .orElseThrow(() -> new JobNotFoundException(orderEvent.getOrderId()));
        }

        // ── 1. Persist Job ───────────────────────────────────────────────
        Job job = Job.builder()
                .orderId(orderEvent.getOrderId())
                .jobType(JobType.ORDER_PROCESSING)
                .status(JobStatus.IN_PROGRESS)
                //.priority(resolvePriority(orderEvent))
                .payload(JSONOperation.serialize(orderEvent))
                //.retryCount(0)
                //.maxRetries(3)
                .scheduledAt(LocalDateTime.now())
                .startedAt(LocalDateTime.now())
                .build();

        job = jobRepository.save(job);

        // correlationId = jobId — the Correlation Key for this saga
        UUID correlationId = job.getJobId();

        // ── 2. Persist initial ProcessingStatus ──────────────────────────
        ProcessingStatus ps = ProcessingStatus.builder()
                .job(job)
                .orderId(orderEvent.getOrderId())
                .currentStep(SagaStep.ORDER_RECEIVED)
                .status(StepStatus.IN_PROGRESS)
                .sagaState(SagaState.STARTED)
                .compensationNeeded(false)
                .lastEvent("ORDER_CREATED")
                .stepStartedAt(LocalDateTime.now())
                .build();

        processingStatusRepository.save(ps);

        // ── 3. Save outgoing event to Outbox (same transaction) ──────────
        outboxService.createEvent(
                correlationId,                          // Correlation Key
                orderEvent.getOrderId(),
                TOPIC_INVENTORY,
                "INVENTORY_CHECK_REQUESTED",
                InventoryCheckEvent.builder()
                        .jobId(correlationId)
                        .orderId(orderEvent.getOrderId())
                        .items(orderEvent.getItems())
                        .build()
        );

        log.info("Saga STARTED jobId={} (correlationId={}) orderId={}",
                job.getJobId(), correlationId, job.getOrderId());
        return job;
    }

    // =========================================================
    // 2. INVENTORY CHECK RESULT
    // =========================================================

    @Transactional
    public void handleInventoryCheckResult(InventoryResultEvent event) {
        log.info("INVENTORY_CHECK_RESULT correlationId={} success={}",
                event.getJobId(), event.isSuccess());

        Job job = jobQueryService.getJobOrThrow(event.getJobId());
        ProcessingStatus ps = getProcessingStatusOrThrow(event.getJobId());
        UUID correlationId = job.getJobId();

        if (event.isSuccess()) {
            ps.advanceTo(SagaStep.INVENTORY_CHECK, "INVENTORY_RESERVED");
            ps.markStepCompleted();
            ps.setSagaState(SagaState.IN_PROGRESS);
            processingStatusRepository.save(ps);

            outboxService.createEvent(
                    correlationId,
                    job.getOrderId(),
                    TOPIC_PAYMENT,            // ← from yaml
                    "PAYMENT_REQUESTED",
                    PaymentRequestEvent.builder()
                            .jobId(correlationId)
                            .orderId(job.getOrderId())
                            .amount(event.getTotalAmount())
                            .currency(event.getCurrency())
                            .build()
            );
        } else {
            failSaga(job, ps, correlationId, "INVENTORY_FAILED",
                    "Inventory check failed: " + event.getFailureReason(), false);
        }
    }

    // =========================================================
    // 3. PAYMENT RESULT
    // =========================================================

    @Transactional
    public void handlePaymentResult(PaymentResultEvent event) {
        log.info("PAYMENT_RESULT correlationId={} success={}",
                event.getJobId(), event.isSuccess());

        Job job = jobQueryService.getJobOrThrow(event.getJobId());
        ProcessingStatus ps = getProcessingStatusOrThrow(event.getJobId());
        UUID correlationId = job.getJobId();

        if (event.isSuccess()) {
            ps.advanceTo(SagaStep.PAYMENT_PROCESSING, "PAYMENT_SUCCESS");
            ps.markStepCompleted();
            ps.setSagaState(SagaState.IN_PROGRESS);
            processingStatusRepository.save(ps);

            outboxService.createEvent(
                    correlationId,
                    job.getOrderId(),
                    TOPIC_ORDER_CONFIRM,       // ← from yaml
                    "ORDER_CONFIRM_REQUESTED",
                    OrderConfirmEvent.builder()
                            .jobId(correlationId)
                            .orderId(job.getOrderId())
                            .transactionId(event.getTransactionId())
                            .build()
            );
        } else {
            failSaga(job, ps, correlationId, "PAYMENT_FAILED",
                    "Payment failed: " + event.getFailureReason(), true);
        }
    }

    // =========================================================
    // 4. ORDER CONFIRM RESULT
    // =========================================================

    @Transactional
    public void handleOrderConfirmResult(OrderConfirmResultEvent event) {
        log.info("ORDER_CONFIRM_RESULT correlationId={} success={}",
                event.getJobId(), event.isSuccess());

        Job job = jobQueryService.getJobOrThrow(event.getJobId());
        ProcessingStatus ps = getProcessingStatusOrThrow(event.getJobId());
        UUID correlationId = job.getJobId();

        if (event.isSuccess()) {
            ps.advanceTo(SagaStep.ORDER_CONFIRMED, "ORDER_CONFIRMED");
            ps.markStepCompleted();
            ps.setSagaState(SagaState.IN_PROGRESS);
            processingStatusRepository.save(ps);

            outboxService.createEvent(
                    correlationId,
                    job.getOrderId(),
                    TOPIC_NOTIFICATION,       // ← from yaml
                    "NOTIFICATION_REQUESTED",
                    NotificationEvent.builder()
                            .jobId(correlationId)
                            .orderId(job.getOrderId())
                            .customerId(event.getCustomerId())
                            .notificationType("ORDER_CONFIRMED")
                            .build()
            );
        } else {
            failSaga(job, ps, correlationId, "ORDER_CONFIRM_FAILED",
                    "Order confirmation failed: " + event.getFailureReason(), true);
        }
    }

    // =========================================================
    // 5. NOTIFICATION RESULT
    // =========================================================

    @Transactional
    public void handleNotificationResult(NotificationResultEvent event) {
        log.info("NOTIFICATION_RESULT correlationId={}", event.getJobId());

        Job job = jobQueryService.getJobOrThrow(event.getJobId());
        ProcessingStatus ps = getProcessingStatusOrThrow(event.getJobId());
        UUID correlationId = job.getJobId();

        if (!event.isSuccess() && NOTIFICATION_CRITICAL) {
            log.warn("Notification failed and is marked critical. Rolling back saga correlationId={}",
                    correlationId);
            failSaga(job, ps, correlationId, "NOTIFICATION_FAILED",
                    "Notification delivery failed", true);
            return;
        }

        if (!event.isSuccess()) {
            log.warn("Notification failed for correlationId={} but non-critical. Completing saga.", correlationId);
        }

        ps.advanceTo(SagaStep.NOTIFICATION_TRIGGERED, "NOTIFICATION_SENT");
        ps.markStepCompleted();
        processingStatusRepository.save(ps);

        completeSaga(job, ps, correlationId);
    }

    // =========================================================
    // 6. SAGA COMPLETION
    // =========================================================

    private void completeSaga(Job job, ProcessingStatus ps, UUID correlationId) {
        ps.advanceTo(SagaStep.COMPLETED, "SAGA_COMPLETED");
        ps.markStepCompleted();
        ps.setSagaState(SagaState.COMPLETED);
        processingStatusRepository.save(ps);

        job.setStatus(JobStatus.COMPLETED);
        job.setCompletedAt(LocalDateTime.now());
        jobRepository.save(job);

        outboxService.createEvent(
                correlationId,
                job.getOrderId(),
                TOPIC_SAGA_RESULT,             // ← from yaml
                "ORDER_PROCESSING_COMPLETED",
                OrderProcessingCompletedEvent.builder()
                        .jobId(correlationId)
                        .orderId(job.getOrderId())
                        .build()
        );

        log.info("Saga COMPLETED jobId={} orderId={}", job.getJobId(), job.getOrderId());
    }

    // =========================================================
    // 7. COMPENSATION
    // =========================================================

    @Transactional
    public void handleCompensation(UUID correlationId) {
        log.warn("Compensation started correlationId={}", correlationId);

        Job job = jobQueryService.getJobOrThrow(correlationId);
        ProcessingStatus ps = getProcessingStatusOrThrow(correlationId);

        ps.setSagaState(SagaState.COMPENSATING);
        ps.setCompensationNeeded(true);
        processingStatusRepository.save(ps);

        String compensationType = switch (ps.getCurrentStep()) {
            case PAYMENT_PROCESSING, ORDER_CONFIRMED -> "PAYMENT_AND_INVENTORY_ROLLBACK";
            case INVENTORY_CHECK                     -> "INVENTORY_ROLLBACK";
            default                                  -> null;
        };

        if (compensationType != null) {
            outboxService.createEvent(
                    correlationId,
                    job.getOrderId(),
                    TOPIC_COMPENSATION,       // ← from yaml
                    compensationType,
                    CompensationEvent.builder()
                            .jobId(correlationId)
                            .orderId(job.getOrderId())
                            .reason(ps.getErrorMessage())
                            .build()
            );
        } else {
            markCompensationDone(job, ps, correlationId);
        }
    }

    @Transactional
    public void handleCompensationComplete(CompensationResultEvent event) {
        log.info("Compensation complete correlationId={}", event.getJobId());
        Job job = jobQueryService.getJobOrThrow(event.getJobId());
        ProcessingStatus ps = getProcessingStatusOrThrow(event.getJobId());
        markCompensationDone(job, ps, event.getJobId());
    }

    private void markCompensationDone(Job job, ProcessingStatus ps, UUID correlationId) {
        ps.setSagaState(SagaState.COMPENSATED);
        ps.setStatus(StepStatus.COMPENSATING);
        ps.setStepCompletedAt(LocalDateTime.now());
        processingStatusRepository.save(ps);

        job.setStatus(JobStatus.FAILED);
        job.setCompletedAt(LocalDateTime.now());
        jobRepository.save(job);

        outboxService.createEvent(
                correlationId,
                job.getOrderId(),
                TOPIC_SAGA_RESULT,             // ← from yaml
                "ORDER_PROCESSING_FAILED",
                OrderProcessingFailedEvent.builder()
                        .jobId(correlationId)
                        .orderId(job.getOrderId())
                        .reason(ps.getErrorMessage())
                        .build()
        );

        log.warn("Saga COMPENSATED jobId={}", job.getJobId());
    }

    // =========================================================
    // 8. RETRY LOGIC
    // =========================================================

    @Transactional
    public void retryJob(UUID jobId) {
        Job job = jobQueryService.getJobOrThrow(jobId);
           // ← from yaml

        if (job.getRetryCount() >= MAX_RETRIES) {
            log.error("Job jobId={} exhausted {} retries. Cancelling.", jobId, MAX_RETRIES);
            job.setStatus(JobStatus.CANCELLED);
            jobRepository.save(job);
            return;
        }

        log.info("Retrying job jobId={} attempt {}/{}",
                jobId, job.getRetryCount() + 1, MAX_RETRIES);

        job.setRetryCount(job.getRetryCount() + 1);
        job.setStatus(JobStatus.RETRYING);
        jobRepository.save(job);

        ProcessingStatus ps = getProcessingStatusOrThrow(jobId);
        ps.setSagaState(SagaState.IN_PROGRESS);
        ps.setStatus(StepStatus.IN_PROGRESS);
        ps.setErrorMessage(null);
        ps.setCompensationNeeded(false);
        processingStatusRepository.save(ps);

        replayStepViaOutbox(job, ps.getCurrentStep());
    }

    // =========================================================
    // PRIVATE HELPERS
    // =========================================================

    private void failSaga(Job job, ProcessingStatus ps, UUID correlationId,
                          String lastEvent, String errorMessage, boolean compensate) {
        ps.markStepFailed(errorMessage, compensate);
        ps.setLastEvent(lastEvent);
        processingStatusRepository.save(ps);

        job.setStatus(JobStatus.FAILED);
        jobRepository.save(job);

        if (compensate) {
            handleCompensation(correlationId);
        } else {
            outboxService.createEvent(
                    correlationId,
                    job.getOrderId(),
                    TOPIC_SAGA_RESULT,         // ← from yaml
                    "ORDER_PROCESSING_FAILED",
                    OrderProcessingFailedEvent.builder()
                            .jobId(correlationId)
                            .orderId(job.getOrderId())
                            .reason(errorMessage)
                            .build()
            );
        }
    }


    private void replayStepViaOutbox(Job job, SagaStep step) {
        UUID correlationId = job.getJobId();

        switch (step) {
            case ORDER_RECEIVED ->
                    outboxService.createEvent(correlationId, job.getOrderId(),
                            TOPIC_INVENTORY, "INVENTORY_CHECK_REQUESTED",
                            InventoryCheckEvent.builder()
                                    .jobId(correlationId).orderId(job.getOrderId()).build());
            case INVENTORY_CHECK ->
                    outboxService.createEvent(correlationId, job.getOrderId(),
                            TOPIC_PAYMENT,"PAYMENT_REQUESTED",
                            PaymentRequestEvent.builder()
                                    .jobId(correlationId).orderId(job.getOrderId()).build());
            case PAYMENT_PROCESSING ->
                    outboxService.createEvent(correlationId, job.getOrderId(),
                            TOPIC_ORDER_CONFIRM, "ORDER_CONFIRM_REQUESTED",
                            OrderConfirmEvent.builder()
                                    .jobId(correlationId).orderId(job.getOrderId()).build());
            case ORDER_CONFIRMED ->
                    outboxService.createEvent(correlationId, job.getOrderId(),
                            TOPIC_NOTIFICATION, "NOTIFICATION_REQUESTED",
                            NotificationEvent.builder()
                                    .jobId(correlationId).orderId(job.getOrderId()).build());
            default -> log.warn("No replay defined for step={}", step);
        }
    }


    private ProcessingStatus getProcessingStatusOrThrow(UUID jobId) {
        return processingStatusRepository.findByJobJobId(jobId)
                .orElseThrow(() -> new SagaException("ProcessingStatus not found for jobId=" + jobId));
    }

    public List<Job> getAllProcessingOrders(String orderStatus) {
        return jobRepository.findByStatus(orderStatus);
                //.orElseThrow(() -> new NoSuchElementException());
    }
}