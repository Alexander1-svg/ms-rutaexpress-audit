package com.ms_rutaexpress.kafka_admin.service;

import com.ms_rutaexpress.kafka_admin.admin.KafkaAdminOperations;
import com.ms_rutaexpress.kafka_admin.config.KafkaAdminProperties;
import com.ms_rutaexpress.kafka_admin.dto.ClusterInfoResponse;
import com.ms_rutaexpress.kafka_admin.dto.CreateTopicRequest;
import com.ms_rutaexpress.kafka_admin.dto.NodeResponse;
import com.ms_rutaexpress.kafka_admin.dto.PartitionInfoResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicConfigEntryResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicConfigsResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicDetailResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicListResponse;
import com.ms_rutaexpress.kafka_admin.dto.UpdateTopicConfigsRequest;
import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import com.ms_rutaexpress.kafka_admin.exception.KafkaUnavailableException;
import com.ms_rutaexpress.kafka_admin.exception.TopicConflictException;
import com.ms_rutaexpress.kafka_admin.validation.TopicConfigValidator;
import com.ms_rutaexpress.kafka_admin.validation.TopicNameValidator;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class KafkaAdministrationService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdministrationService.class);

    private final KafkaAdminOperations operations;
    private final KafkaAdminProperties properties;
    private final TopicNameValidator topicNameValidator;
    private final TopicConfigValidator configValidator;

    public KafkaAdministrationService(
            KafkaAdminOperations operations,
            KafkaAdminProperties properties,
            TopicNameValidator topicNameValidator,
            TopicConfigValidator configValidator
    ) {
        this.operations = operations;
        this.properties = properties;
        this.topicNameValidator = topicNameValidator;
        this.configValidator = configValidator;
    }

    public ClusterInfoResponse cluster() {
        var metadata = operations.describeCluster();
        List<String> topics = sortedTopics();
        return new ClusterInfoResponse(
                metadata.clusterId(),
                node(metadata.controller()),
                metadata.brokers().stream().map(this::node).sorted(Comparator.comparingInt(NodeResponse::id)).toList(),
                topics.size(),
                topics,
                properties.analyticsTopics()
        );
    }

    public TopicListResponse listTopics() {
        List<String> topics = sortedTopics();
        return new TopicListResponse(topics.size(), topics);
    }

    public TopicDetailResponse topic(String topicName) {
        topicNameValidator.validate(topicName);
        return topicResponse(operations.describeTopic(topicName));
    }

    public TopicDetailResponse createTopic(CreateTopicRequest request) {
        requireWritesEnabled();
        topicNameValidator.validateForCreation(request.name());

        int partitions = request.partitions() == null
                ? properties.defaultPartitions()
                : request.partitions();
        short replicationFactor = request.replicationFactor() == null
                ? properties.defaultReplicationFactor()
                : request.replicationFactor();

        configValidator.validate(request.configs());
        int brokerCount = operations.describeCluster().brokers().size();
        if (brokerCount == 0) {
            throw new KafkaUnavailableException("Kafka no informó brokers disponibles", null);
        }
        if (replicationFactor > brokerCount) {
            throw new InvalidAdminRequestException(
                    "El factor de replicación no puede superar los brokers disponibles: " + brokerCount
            );
        }
        validateMinInSyncReplicas(request.configs(), replicationFactor);

        operations.createTopic(request.name(), partitions, replicationFactor, request.configs());
        log.info(
                "Tópico Kafka creado: topic={}, partitions={}, replicationFactor={}",
                request.name(), partitions, replicationFactor
        );
        return topic(request.name());
    }

    public TopicConfigsResponse topicConfigs(String topicName) {
        topicNameValidator.validate(topicName);
        operations.describeTopic(topicName);
        return configsResponse(topicName, operations.describeTopicConfigs(topicName));
    }

    public TopicConfigsResponse updateTopicConfigs(String topicName, UpdateTopicConfigsRequest request) {
        requireWritesEnabled();
        topicNameValidator.validate(topicName);
        configValidator.validate(request.configs());

        TopicDescription description = operations.describeTopic(topicName);
        int minimumReplicationFactor = description.partitions().stream()
                .mapToInt(partition -> partition.replicas().size())
                .min()
                .orElseThrow(() -> new TopicConflictException("El tópico no tiene particiones"));
        validateMinInSyncReplicas(request.configs(), minimumReplicationFactor);

        operations.updateTopicConfigs(topicName, request.configs());
        log.info("Configuraciones Kafka actualizadas: topic={}, keys={}", topicName, request.configs().keySet());
        return topicConfigs(topicName);
    }

    public TopicDetailResponse increasePartitions(String topicName, int totalPartitions) {
        requireWritesEnabled();
        topicNameValidator.validate(topicName);
        TopicDescription description = operations.describeTopic(topicName);
        int current = description.partitions().size();
        if (totalPartitions <= current) {
            throw new TopicConflictException(
                    "Las particiones solo pueden aumentar; cantidad actual: " + current
            );
        }
        operations.increasePartitions(topicName, totalPartitions);
        log.info("Particiones Kafka aumentadas: topic={}, from={}, to={}", topicName, current, totalPartitions);
        return topic(topicName);
    }

    public void deleteTopic(String topicName) {
        requireWritesEnabled();
        topicNameValidator.validate(topicName);
        if (isProtected(topicName) || topicName.startsWith("__")) {
            throw new TopicConflictException("El tópico está protegido y no puede eliminarse: " + topicName);
        }
        operations.describeTopic(topicName);
        operations.deleteTopic(topicName);
        log.info("Tópico Kafka eliminado: topic={}", topicName);
    }

    private List<String> sortedTopics() {
        return operations.listTopics().stream().sorted().toList();
    }

    private TopicDetailResponse topicResponse(TopicDescription description) {
        List<PartitionInfoResponse> partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(partition -> new PartitionInfoResponse(
                        partition.partition(),
                        node(partition.leader()),
                        partition.replicas().stream().map(this::node).toList(),
                        partition.isr().stream().map(this::node).toList()
                ))
                .toList();
        List<Integer> replicationFactors = description.partitions().stream()
                .map(partition -> partition.replicas().size())
                .distinct()
                .sorted()
                .toList();
        return new TopicDetailResponse(
                description.name(),
                description.isInternal(),
                isProtected(description.name()),
                description.partitions().size(),
                replicationFactors,
                partitions
        );
    }

    private TopicConfigsResponse configsResponse(String topicName, Config config) {
        List<TopicConfigEntryResponse> entries = config.entries().stream()
                .sorted(Comparator.comparing(org.apache.kafka.clients.admin.ConfigEntry::name))
                .map(entry -> new TopicConfigEntryResponse(
                        entry.name(),
                        entry.isSensitive() ? null : entry.value(),
                        entry.source().name(),
                        entry.isReadOnly(),
                        entry.isSensitive()
                ))
                .toList();
        return new TopicConfigsResponse(topicName, entries);
    }

    private NodeResponse node(Node node) {
        if (node == null || node.isEmpty()) {
            return null;
        }
        return new NodeResponse(node.id(), node.host(), node.port(), node.rack());
    }

    private boolean isProtected(String topicName) {
        return properties.protectedTopics().contains(topicName);
    }

    private void validateMinInSyncReplicas(Map<String, String> configs, int replicationFactor) {
        int minInSyncReplicas = configValidator.minInSyncReplicas(configs);
        if (minInSyncReplicas > replicationFactor) {
            throw new InvalidAdminRequestException(
                    "min.insync.replicas no puede superar el factor de replicación: " + replicationFactor
            );
        }
    }

    private void requireWritesEnabled() {
        if (!properties.writeEnabled()) {
            throw new TopicConflictException("Las operaciones administrativas de escritura están deshabilitadas");
        }
    }
}
