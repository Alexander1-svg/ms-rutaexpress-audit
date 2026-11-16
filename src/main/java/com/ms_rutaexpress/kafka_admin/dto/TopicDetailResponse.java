package com.ms_rutaexpress.kafka_admin.dto;

import java.util.List;

public record TopicDetailResponse(
        String name,
        boolean internal,
        boolean protectedTopic,
        int partitionCount,
        List<Integer> replicationFactors,
        List<PartitionInfoResponse> partitions
) {
}

