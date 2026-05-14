package org.assignments.processing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONOperation {

    public static String serialize(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize outbox payload", e);
        }
    }
}
