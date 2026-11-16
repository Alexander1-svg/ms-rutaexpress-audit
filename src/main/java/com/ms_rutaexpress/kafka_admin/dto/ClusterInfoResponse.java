package com.ms_rutaexpress.kafka_admin.dto;

import java.util.List;

public record ClusterInfoResponse(
        String clusterId,
        NodeResponse controller,
        List<NodeResponse> brokers,
        int topicCount,
        List<String> topics,
        List<String> analyticsTopics
) {
}

