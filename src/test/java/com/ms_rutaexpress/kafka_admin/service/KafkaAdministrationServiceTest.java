package com.ms_rutaexpress.kafka_admin.service;

import com.ms_rutaexpress.kafka_admin.admin.ClusterMetadata;
import com.ms_rutaexpress.kafka_admin.admin.KafkaAdminOperations;
import com.ms_rutaexpress.kafka_admin.config.KafkaAdminProperties;
import com.ms_rutaexpress.kafka_admin.dto.CreateTopicRequest;
import com.ms_rutaexpress.kafka_admin.dto.UpdateTopicConfigsRequest;
import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import com.ms_rutaexpress.kafka_admin.exception.TopicConflictException;
import com.ms_rutaexpress.kafka_admin.validation.TopicConfigValidator;
import com.ms_rutaexpress.kafka_admin.validation.TopicNameValidator;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAdministrationServiceTest {

    private final KafkaAdminOperations operations = mock(KafkaAdminOperations.class);
    private final KafkaAdministrationService service = new KafkaAdministrationService(
            operations,
            properties(true),
            new TopicNameValidator(),
            new TopicConfigValidator()
    );

    @Test
    void listsTopicsInAlphabeticalOrder() {
        when(operations.listTopics()).thenReturn(Set.of("z-topic", "a-topic"));

        var response = service.listTopics();

        assertThat(response.count()).isEqualTo(2);
        assertThat(response.topics()).containsExactly("a-topic", "z-topic");
    }

    @Test
    void returnsTopicPartitionsReplicasAndAssignments() {
        when(operations.describeTopic("shipments")).thenReturn(description("shipments", 2, 2));

        var response = service.topic("shipments");

        assertThat(response.partitionCount()).isEqualTo(2);
        assertThat(response.replicationFactors()).containsExactly(2);
        assertThat(response.partitions().getFirst().replicas()).hasSize(2);
        assertThat(response.partitions().getFirst().inSyncReplicas()).hasSize(2);
    }

    @Test
    void createsTopicUsingDefaultsAndValidatedConfigs() {
        when(operations.describeCluster()).thenReturn(cluster(3));
        when(operations.describeTopic("rutaexpress.test.v1"))
                .thenReturn(description("rutaexpress.test.v1", 3, 1));
        CreateTopicRequest request = new CreateTopicRequest(
                "rutaexpress.test.v1", null, null, Map.of("retention.ms", "60000")
        );

        var response = service.createTopic(request);

        verify(operations).createTopic(
                "rutaexpress.test.v1", 3, (short) 1, Map.of("retention.ms", "60000")
        );
        assertThat(response.name()).isEqualTo("rutaexpress.test.v1");
    }

    @Test
    void rejectsReplicationFactorGreaterThanAvailableBrokers() {
        when(operations.describeCluster()).thenReturn(cluster(1));
        CreateTopicRequest request = new CreateTopicRequest("test-topic", 2, (short) 2, Map.of());

        assertThatThrownBy(() -> service.createTopic(request))
                .isInstanceOf(InvalidAdminRequestException.class)
                .hasMessageContaining("brokers disponibles");
        verify(operations, never()).createTopic(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyShort(),
                org.mockito.ArgumentMatchers.anyMap()
        );
    }

    @Test
    void rejectsImmutableOrUnsupportedConfiguration() {
        var request = new UpdateTopicConfigsRequest(Map.of("segment.bytes", "1024"));

        assertThatThrownBy(() -> service.updateTopicConfigs("test-topic", request))
                .isInstanceOf(InvalidAdminRequestException.class)
                .hasMessageContaining("Configuración no permitida");
    }

    @Test
    void rejectsMinInSyncReplicasGreaterThanReplicationFactor() {
        when(operations.describeTopic("test-topic")).thenReturn(description("test-topic", 1, 2));
        var request = new UpdateTopicConfigsRequest(Map.of("min.insync.replicas", "3"));

        assertThatThrownBy(() -> service.updateTopicConfigs("test-topic", request))
                .isInstanceOf(InvalidAdminRequestException.class)
                .hasMessageContaining("factor de replicación");
    }

    @Test
    void updatesAllowedConfigurationsAndRedactsSensitiveValues() {
        when(operations.describeTopic("test-topic")).thenReturn(description("test-topic", 1, 2));
        ConfigEntry sensitiveEntry = mock(ConfigEntry.class);
        when(sensitiveEntry.name()).thenReturn("secret.config");
        when(sensitiveEntry.value()).thenReturn("hidden");
        when(sensitiveEntry.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
        when(sensitiveEntry.isSensitive()).thenReturn(true);
        Config topicConfig = new Config(List.of(
                new ConfigEntry("retention.ms", "60000"),
                sensitiveEntry
        ));
        when(operations.describeTopicConfigs("test-topic")).thenReturn(topicConfig);
        var request = new UpdateTopicConfigsRequest(Map.of(
                "retention.ms", "60000",
                "cleanup.policy", "delete,compact"
        ));

        var response = service.updateTopicConfigs("test-topic", request);

        verify(operations).updateTopicConfigs("test-topic", request.configs());
        assertThat(response.configs()).filteredOn(entry -> entry.name().equals("secret.config"))
                .singleElement()
                .satisfies(entry -> {
                    assertThat(entry.sensitive()).isTrue();
                    assertThat(entry.value()).isNull();
                });
    }

    @Test
    void increasesButNeverReducesPartitions() {
        when(operations.describeTopic("test-topic"))
                .thenReturn(description("test-topic", 2, 1))
                .thenReturn(description("test-topic", 4, 1));

        var response = service.increasePartitions("test-topic", 4);

        verify(operations).increasePartitions("test-topic", 4);
        assertThat(response.partitionCount()).isEqualTo(4);

        when(operations.describeTopic("other-topic")).thenReturn(description("other-topic", 3, 1));
        assertThatThrownBy(() -> service.increasePartitions("other-topic", 2))
                .isInstanceOf(TopicConflictException.class)
                .hasMessageContaining("solo pueden aumentar");
    }

    @Test
    void protectsAnalyticsTopicsFromDeletion() {
        assertThatThrownBy(() -> service.deleteTopic("rutaexpress.shipments.events.v1"))
                .isInstanceOf(TopicConflictException.class)
                .hasMessageContaining("protegido");
        verify(operations, never()).deleteTopic("rutaexpress.shipments.events.v1");
    }

    @Test
    void deletesExistingUnprotectedTopic() {
        when(operations.describeTopic("temporary-topic"))
                .thenReturn(description("temporary-topic", 1, 1));

        service.deleteTopic("temporary-topic");

        verify(operations).deleteTopic("temporary-topic");
    }

    @Test
    void disablesAllWriteOperationsWhenConfigured() {
        KafkaAdministrationService readOnlyService = new KafkaAdministrationService(
                operations,
                properties(false),
                new TopicNameValidator(),
                new TopicConfigValidator()
        );

        assertThatThrownBy(() -> readOnlyService.deleteTopic("test-topic"))
                .isInstanceOf(TopicConflictException.class)
                .hasMessageContaining("deshabilitadas");
    }

    private KafkaAdminProperties properties(boolean writeEnabled) {
        return new KafkaAdminProperties(
                List.of("localhost:9092"), "PLAINTEXT", Duration.ofSeconds(2),
                3, (short) 1, writeEnabled,
                List.of(
                        "rutaexpress.shipments.events.v1",
                        "rutaexpress.shipments.analytics.v1",
                        "rutaexpress.shipments.events.v1.DLT"
                ),
                List.of(
                        "rutaexpress.shipments.events.v1",
                        "rutaexpress.shipments.analytics.v1",
                        "rutaexpress.shipments.events.v1.DLT",
                        "__consumer_offsets"
                )
        );
    }

    private ClusterMetadata cluster(int brokerCount) {
        List<Node> brokers = new ArrayList<>();
        for (int index = 0; index < brokerCount; index++) {
            brokers.add(new Node(index, "broker-" + index, 9092 + index));
        }
        return new ClusterMetadata("cluster-1", brokers.isEmpty() ? null : brokers.getFirst(), brokers);
    }

    private TopicDescription description(String name, int partitions, int replicationFactor) {
        List<Node> replicas = new ArrayList<>();
        for (int index = 0; index < replicationFactor; index++) {
            replicas.add(new Node(index, "broker-" + index, 9092 + index));
        }
        List<TopicPartitionInfo> partitionInfos = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            partitionInfos.add(new TopicPartitionInfo(partition, replicas.getFirst(), replicas, replicas));
        }
        return new TopicDescription(name, false, partitionInfos);
    }
}

