CREATE TABLE IF NOT EXISTS jobs (
    job_id          CHAR(36)        NOT NULL DEFAULT (UUID()),
    order_id        CHAR(36)        NOT NULL,
    job_type        VARCHAR(50)     NOT NULL,
    status          VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
    priority        VARCHAR(50)     NOT NULL DEFAULT 'MEDIUM',
    -- Full order snapshot; decouples processing from Order service
    payload         JSON            NOT NULL,
    retry_count     INT             NOT NULL DEFAULT 0,
    max_retries     INT             NOT NULL DEFAULT 3,
    scheduled_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at      DATETIME        NULL,
    completed_at    DATETIME        NULL,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT (pk_jobs)                      PRIMARY KEY (job_id)
--     CONSTRAINT chk_retry_count              CHECK (retry_count >= 0),
--     CONSTRAINT chk_max_retries              CHECK (max_retries > 0),
--     CONSTRAINT chk_retry_within_max         CHECK (retry_count <= max_retries),
--     CONSTRAINT chk_started_after_scheduled  CHECK (started_at IS NULL OR started_at >= scheduled_at),
--     CONSTRAINT chk_completed_after_started  CHECK (completed_at IS NULL OR started_at IS NOT NULL)

    )
    COMMENT='Represents a unit of work for processing an order in the saga';

-- Indexes for jobs
CREATE INDEX idx_jobs_order_id      ON jobs (order_id);
CREATE INDEX idx_jobs_status        ON jobs (status);
-- CREATE INDEX idx_jobs_priority      ON jobs (priority);
CREATE INDEX idx_jobs_scheduled_at  ON jobs (scheduled_at);

-- Functional index on JSON payload to query by orderId inside the snapshot
CREATE INDEX idx_jobs_payload_order ON jobs ((CAST(payload->>'$.orderId' AS CHAR(36))));


-- -------------------------------------------------------------
-- TABLE: processing_status
-- Tracks the saga step and compensation state per job.
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS processing_status (
    status_id           CHAR(36)    NOT NULL DEFAULT (UUID()),
    job_id              CHAR(36)    NOT NULL,
    order_id            CHAR(36)    NOT NULL,

    current_step        VARCHAR(50)           NOT NULL,

    status              VARCHAR(50)           NOT NULL DEFAULT 'IN_PROGRESS',

    saga_state          VARCHAR(50)           NOT NULL DEFAULT 'STARTED',

    compensation_needed TINYINT(1)  NOT NULL DEFAULT 0,

    approval_status NOT NULL DEFAULT 'NOT_REQUIRED',
    approved_by       VARCHAR(150) NULL,
    approval_remarks  TEXT NULL,
    approved_at       DATETIME NULL

    -- Event that triggered the last step transition
    -- e.g. ORDER_CREATED, PAYMENT_SUCCESS, INVENTORY_FAILED
    last_event          VARCHAR(100) NULL,

    error_message       TEXT         NULL,

    step_started_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    step_completed_at   DATETIME    NULL,

    created_at          DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_processing_status         PRIMARY KEY (status_id),
    CONSTRAINT fk_ps_job  FOREIGN KEY (job_id) REFERENCES jobs(job_id)
                                                                       ON DELETE CASCADE
                                                                       ON UPDATE CASCADE,
--     CONSTRAINT chk_step_completed_after_start
--     CHECK (step_completed_at IS NULL OR step_completed_at >= step_started_at),
--     CONSTRAINT chk_compensation_state
--     CHECK (
--               compensation_needed = 0
--               OR saga_state IN ('COMPENSATING', 'COMPENSATED', 'FAILED')
--     )

    )
    COMMENT='Tracks current saga step and compensation state for each job';

-- Indexes for processing_status
CREATE INDEX idx_ps_job_id      ON processing_status (job_id);
CREATE INDEX idx_ps_order_id    ON processing_status (order_id);
CREATE INDEX idx_ps_saga_state  ON processing_status (saga_state);
CREATE INDEX idx_ps_step        ON processing_status (current_step);

-- =============================================================
-- Outbox Events Table — MySQL 8.0+
-- Stores all outgoing events atomically with business state.
-- The OutboxPoller relays PENDING events to Kafka.
-- =============================================================

CREATE TABLE IF NOT EXISTS outbox_events (
                                             event_id            CHAR(36)        NOT NULL DEFAULT (UUID()),

    -- Correlation Key: always = jobId, ties all saga events together
    correlation_id      CHAR(36)        NOT NULL,

    order_id            CHAR(36)        NOT NULL,

    topic               VARCHAR(100)    NOT NULL COMMENT 'Target Kafka topic',
    event_type          VARCHAR(100)    NOT NULL COMMENT 'e.g. INVENTORY_CHECK_REQUESTED',

    -- Serialized JSON event payload
    payload             JSON            NOT NULL,

    status              VARCHAR(100)               NOT NULL DEFAULT 'PENDING',

    -- Kafka partition key: defaults to correlation_id for ordered per-saga delivery
    partition_key       VARCHAR(100)    NOT NULL,

    retry_count         INT             NOT NULL DEFAULT 0,
    max_retries         INT             NOT NULL DEFAULT 3,
    error_message       TEXT            NULL,

    published_at        DATETIME        NULL,
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT pk_outbox_events         PRIMARY KEY (event_id),
    CONSTRAINT uq_correlation_event     UNIQUE (correlation_id, event_type),  -- idempotency guard
    CONSTRAINT chk_retry_count          CHECK (retry_count >= 0),
    CONSTRAINT chk_retry_within_max     CHECK (retry_count <= max_retries)

    )
    COMMENT='Outbox table: events written atomically, published to Kafka by OutboxPoller';

-- Indexes
CREATE INDEX idx_outbox_status          ON outbox_events (status);
CREATE INDEX idx_outbox_correlation_id  ON outbox_events (correlation_id);
CREATE INDEX idx_outbox_order_id        ON outbox_events (order_id);
CREATE INDEX idx_outbox_created_at      ON outbox_events (created_at);

-- Composite: poller query — PENDING events with retries remaining
CREATE INDEX idx_outbox_poll            ON outbox_events (status, retry_count, created_at);