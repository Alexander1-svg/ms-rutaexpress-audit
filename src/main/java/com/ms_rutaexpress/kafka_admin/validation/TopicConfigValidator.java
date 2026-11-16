package com.ms_rutaexpress.kafka_admin.validation;

import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Component
public class TopicConfigValidator {

    private final Map<String, Consumer<String>> validators = Map.of(
            "retention.ms", value -> validateLongAtLeastMinusOne("retention.ms", value),
            "retention.bytes", value -> validateLongAtLeastMinusOne("retention.bytes", value),
            "cleanup.policy", this::validateCleanupPolicy,
            "min.insync.replicas", this::validateMinInSyncReplicas
    );

    public void validate(Map<String, String> configs) {
        if (configs == null) {
            throw new InvalidAdminRequestException("Las configuraciones son obligatorias");
        }
        configs.forEach((name, value) -> {
            Consumer<String> validator = validators.get(name);
            if (validator == null) {
                throw new InvalidAdminRequestException("Configuración no permitida: " + name);
            }
            if (value == null || value.isBlank()) {
                throw new InvalidAdminRequestException("Valor vacío para la configuración: " + name);
            }
            validator.accept(value.trim());
        });
    }

    public int minInSyncReplicas(Map<String, String> configs) {
        String value = configs.get("min.insync.replicas");
        return value == null ? -1 : Integer.parseInt(value.trim());
    }

    private void validateLongAtLeastMinusOne(String name, String value) {
        try {
            if (Long.parseLong(value) < -1) {
                throw new InvalidAdminRequestException(name + " debe ser -1 o un número no negativo");
            }
        } catch (NumberFormatException exception) {
            throw new InvalidAdminRequestException(name + " debe ser numérico");
        }
    }

    private void validateCleanupPolicy(String value) {
        Set<String> policies = Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(policy -> !policy.isEmpty())
                .collect(Collectors.toSet());
        if (policies.isEmpty() || !Set.of("delete", "compact").containsAll(policies)) {
            throw new InvalidAdminRequestException("cleanup.policy solo admite delete, compact o ambos");
        }
    }

    private void validateMinInSyncReplicas(String value) {
        try {
            if (Integer.parseInt(value) < 1) {
                throw new InvalidAdminRequestException("min.insync.replicas debe ser al menos 1");
            }
        } catch (NumberFormatException exception) {
            throw new InvalidAdminRequestException("min.insync.replicas debe ser numérico");
        }
    }
}

