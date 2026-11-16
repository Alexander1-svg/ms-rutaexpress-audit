package com.ms_rutaexpress.kafka_admin.dto;

import jakarta.validation.constraints.NotEmpty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record UpdateTopicConfigsRequest(@NotEmpty Map<String, String> configs) {
    public UpdateTopicConfigsRequest {
        configs = configs == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(configs));
    }
}

