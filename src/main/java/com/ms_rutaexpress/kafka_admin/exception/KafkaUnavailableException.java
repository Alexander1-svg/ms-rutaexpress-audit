package com.ms_rutaexpress.kafka_admin.exception;

public class KafkaUnavailableException extends RuntimeException {
    public KafkaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

