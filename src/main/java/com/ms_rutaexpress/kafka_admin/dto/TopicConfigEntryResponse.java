package com.ms_rutaexpress.kafka_admin.dto;

public record TopicConfigEntryResponse(
        String name,
        String value,
        String source,
        boolean readOnly,
        boolean sensitive
) {
}

