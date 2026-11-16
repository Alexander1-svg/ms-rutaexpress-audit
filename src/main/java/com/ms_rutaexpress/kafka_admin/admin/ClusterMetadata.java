package com.ms_rutaexpress.kafka_admin.admin;

import org.apache.kafka.common.Node;

import java.util.List;

public record ClusterMetadata(String clusterId, Node controller, List<Node> brokers) {
}

