package com.ms_rutaexpress.kafka_admin.dto;

import java.util.List;

public record TopicConfigsResponse(String topicName, List<TopicConfigEntryResponse> configs) {
}

