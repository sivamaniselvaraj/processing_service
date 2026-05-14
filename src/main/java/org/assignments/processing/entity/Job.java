package org.assignments.processing.entity;

import com.fasterxml.jackson.annotation.JsonManagedReference;

import jakarta.persistence.*;
import lombok.*;
import org.assignments.processing.enums.JobStatus;
import org.assignments.processing.enums.JobType;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "jobs")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@DynamicUpdate
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "job_id", updatable = false, nullable = false)
    private UUID jobId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type", nullable = false, length = 50)
    private JobType jobType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private JobStatus status;

//    @Enumerated(EnumType.STRING)
//    @Column(name = "priority", nullable = false, length = 20)
//    private JobPriority priority;

    /**
     * Full order snapshot stored as JSONB.
     * This ensures processing is independent of the Order service.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "json")
    private String payload;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private int retryCount = 0;

    @Column(name = "max_retries", nullable = false)
    @Builder.Default
    private int maxRetries = 3;

    @Column(name = "scheduled_at")
    private LocalDateTime scheduledAt;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @JsonManagedReference
    @OneToMany(mappedBy = "job", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<ProcessingStatus> processingStatuses;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        if (this.scheduledAt == null) {
            this.scheduledAt = LocalDateTime.now();
        }
        if (this.status == null) {
            this.status = JobStatus.PENDING;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

}