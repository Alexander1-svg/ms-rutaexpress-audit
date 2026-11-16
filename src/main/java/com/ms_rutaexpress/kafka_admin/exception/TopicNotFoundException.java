package com.ms_rutaexpress.kafka_admin.exception;

public class TopicNotFoundException extends RuntimeException {
    public TopicNotFoundException(String topicName) {
        super("El tópico no existe: " + topicName);
    }
}

