package com.ms_rutaexpress.kafka_admin.dto;

import java.util.List;

public record PartitionInfoResponse(
        int partition,
        NodeResponse leader,
        List<NodeResponse> replicas,
        List<NodeResponse> inSyncReplicas
) {
}

