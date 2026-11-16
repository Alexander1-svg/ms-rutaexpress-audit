package com.ms_rutaexpress.kafka_admin.exception;

public class KafkaAdminOperationException extends RuntimeException {
    public KafkaAdminOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}

