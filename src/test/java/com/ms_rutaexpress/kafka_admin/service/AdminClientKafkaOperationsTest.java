package com.ms_rutaexpress.kafka_admin.service;

import com.ms_rutaexpress.kafka_admin.admin.AdminClientKafkaOperations;
import com.ms_rutaexpress.kafka_admin.config.KafkaAdminProperties;
import com.ms_rutaexpress.kafka_admin.exception.KafkaUnavailableException;
import com.ms_rutaexpress.kafka_admin.exception.TopicConflictException;
import com.ms_rutaexpress.kafka_admin.exception.TopicNotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class AdminClientKafkaOperationsTest {

    private final Admin admin = mock(Admin.class);
    private final AdminClientKafkaOperations operations = new AdminClientKafkaOperations(admin, properties());

    @SuppressWarnings("unchecked")
    @Test
    void mapsClientTimeoutToKafkaUnavailable() throws Exception {
        ListTopicsResult result = mock(ListTopicsResult.class);
        KafkaFuture<Set<String>> future = mock(KafkaFuture.class);
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(result);
        when(result.names()).thenReturn(future);
        when(future.get(100, TimeUnit.MILLISECONDS))
                .thenThrow(new java.util.concurrent.TimeoutException("timeout"));

        assertThatThrownBy(operations::listTopics)
                .isInstanceOf(KafkaUnavailableException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapsDuplicateTopicFromKafkaToConflict() throws Exception {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(admin.createTopics(any())).thenReturn(result);
        when(result.all()).thenReturn(future);
        when(future.get(100, TimeUnit.MILLISECONDS))
                .thenThrow(new ExecutionException(new TopicExistsException("exists")));

        assertThatThrownBy(() -> operations.createTopic("duplicate", 1, (short) 1, Map.of()))
                .isInstanceOf(TopicConflictException.class)
                .hasMessageContaining("ya existe");
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapsUnknownTopicFromKafkaToNotFound() throws Exception {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> future = mock(KafkaFuture.class);
        when(admin.describeTopics(List.of("missing"))).thenReturn(result);
        when(result.topicNameValues()).thenReturn(Map.of("missing", future));
        when(future.get(100, TimeUnit.MILLISECONDS))
                .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("missing")));

        assertThatThrownBy(() -> operations.describeTopic("missing"))
                .isInstanceOf(TopicNotFoundException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void deletesTopicThroughKafkaAdminClientAndWaitsForCompletion() throws Exception {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(admin.deleteTopics(List.of("temporary-topic"))).thenReturn(result);
        when(result.all()).thenReturn(future);
        when(future.get(100, TimeUnit.MILLISECONDS)).thenReturn(null);

        operations.deleteTopic("temporary-topic");

        verify(admin).deleteTopics(List.of("temporary-topic"));
        verify(future).get(100, TimeUnit.MILLISECONDS);
    }

    private KafkaAdminProperties properties() {
        return new KafkaAdminProperties(
                List.of("localhost:9092"), "PLAINTEXT", Duration.ofMillis(100),
                3, (short) 1, true, List.of(), List.of()
        );
    }
}
