package org.assignments.processing.exception;

import lombok.Builder;
import lombok.Getter;
import org.assignments.processing.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleJobNotFound(JobNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(error(HttpStatus.NOT_FOUND, ex.getMessage()));
    }

    @ExceptionHandler(ApprovalException.class)
    public ResponseEntity<ErrorResponse> handleApprovalException(ApprovalException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(error(HttpStatus.CONFLICT, ex.getMessage()));
    }

    @ExceptionHandler(SagaException.class)
    public ResponseEntity<ErrorResponse> handleSagaException(SagaException ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = new HashMap<>();
        for (FieldError fe : ex.getBindingResult().getFieldErrors()) {
            fieldErrors.put(fe.getField(), fe.getDefaultMessage());
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.builder()
                        .status(HttpStatus.BAD_REQUEST.value())
                        .error("Validation Failed")
                        .message(fieldErrors.toString())
                        .timestamp(LocalDateTime.now())
                        .build());
    }

    private ErrorResponse error(HttpStatus status, String message) {
        return ErrorResponse.builder()
                .status(status.value())
                .error(status.getReasonPhrase())
                .message(message)
                .timestamp(LocalDateTime.now())
                .build();
    }

}