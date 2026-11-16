package com.ms_rutaexpress.kafka_admin.dto;

import java.util.List;

public record TopicListResponse(int count, List<String> topics) {
}

