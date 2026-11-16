package com.ms_rutaexpress.kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record CreateTopicRequest(
        @NotBlank @Size(max = 249) String name,
        @Min(1) Integer partitions,
        @Min(1) Short replicationFactor,
        Map<String, String> configs
) {
    public CreateTopicRequest {
        configs = configs == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(configs));
    }
}

