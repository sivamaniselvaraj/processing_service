package org.assignments.processing.enums;

public enum OutboxStatus {
    PENDING,       // waiting to be published
    PUBLISHED,     // successfully sent to Kafka
    FAILED,        // exhausted retries
    SKIPPED        // manually skipped
}
