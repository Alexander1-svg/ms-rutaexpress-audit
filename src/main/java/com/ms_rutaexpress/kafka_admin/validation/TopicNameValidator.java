package com.ms_rutaexpress.kafka_admin.validation;

import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class TopicNameValidator {

    private static final Pattern VALID_NAME = Pattern.compile("[a-zA-Z0-9._-]+");

    public void validate(String topicName) {
        if (topicName == null
                || topicName.isBlank()
                || topicName.length() > 249
                || topicName.equals(".")
                || topicName.equals("..")
                || !VALID_NAME.matcher(topicName).matches()) {
            throw new InvalidAdminRequestException("Nombre de tópico inválido");
        }
    }

    public void validateForCreation(String topicName) {
        validate(topicName);
        if (topicName.startsWith("__")) {
            throw new InvalidAdminRequestException("No se permite crear tópicos del espacio interno de Kafka");
        }
    }
}

