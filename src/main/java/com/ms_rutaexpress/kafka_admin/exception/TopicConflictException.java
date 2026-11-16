package com.ms_rutaexpress.kafka_admin.exception;

public class TopicConflictException extends RuntimeException {
    public TopicConflictException(String message) {
        super(message);
    }
}

