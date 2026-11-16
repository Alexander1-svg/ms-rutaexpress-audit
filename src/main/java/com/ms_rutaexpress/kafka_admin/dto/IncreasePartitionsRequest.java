package com.ms_rutaexpress.kafka_admin.dto;

import jakarta.validation.constraints.Min;

public record IncreasePartitionsRequest(@Min(1) int totalPartitions) {
}

