package org.assignments.processing.exception;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("Job not found for jobId=" + jobId);
    }
    public JobNotFoundException(UUID jobId) {
        super("Job not found for jobId=" + jobId);
    }
}