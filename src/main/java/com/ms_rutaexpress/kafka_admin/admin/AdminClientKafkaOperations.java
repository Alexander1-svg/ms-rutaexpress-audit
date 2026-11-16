package com.ms_rutaexpress.kafka_admin.admin;

import com.ms_rutaexpress.kafka_admin.config.KafkaAdminProperties;
import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import com.ms_rutaexpress.kafka_admin.exception.KafkaAdminOperationException;
import com.ms_rutaexpress.kafka_admin.exception.KafkaUnavailableException;
import com.ms_rutaexpress.kafka_admin.exception.TopicConflictException;
import com.ms_rutaexpress.kafka_admin.exception.TopicNotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class AdminClientKafkaOperations implements KafkaAdminOperations {

    private final Admin admin;
    private final Duration timeout;

    public AdminClientKafkaOperations(Admin admin, KafkaAdminProperties properties) {
        this.admin = admin;
        this.timeout = properties.requestTimeout();
    }

    @Override
    public Set<String> listTopics() {
        return await(admin.listTopics(new ListTopicsOptions().listInternal(true)).names(), null);
    }

    @Override
    public TopicDescription describeTopic(String topicName) {
        KafkaFuture<TopicDescription> future = admin.describeTopics(List.of(topicName))
                .topicNameValues()
                .get(topicName);
        if (future == null) {
            throw new TopicNotFoundException(topicName);
        }
        return await(future, topicName);
    }

    @Override
    public Config describeTopicConfigs(String topicName) {
        ConfigResource resource = topicResource(topicName);
        KafkaFuture<Config> future = admin.describeConfigs(List.of(resource)).values().get(resource);
        if (future == null) {
            throw new TopicNotFoundException(topicName);
        }
        return await(future, topicName);
    }

    @Override
    public ClusterMetadata describeCluster() {
        var result = admin.describeCluster();
        Collection<org.apache.kafka.common.Node> nodes = await(result.nodes(), null);
        return new ClusterMetadata(
                await(result.clusterId(), null),
                await(result.controller(), null),
                List.copyOf(nodes)
        );
    }

    @Override
    public void createTopic(
            String topicName,
            int partitions,
            short replicationFactor,
            Map<String, String> configs
    ) {
        NewTopic topic = new NewTopic(topicName, partitions, replicationFactor).configs(configs);
        await(admin.createTopics(List.of(topic)).all(), topicName);
    }

    @Override
    public void deleteTopic(String topicName) {
        await(admin.deleteTopics(List.of(topicName)).all(), topicName);
    }

    @Override
    public void increasePartitions(String topicName, int totalPartitions) {
        await(
                admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(totalPartitions))).all(),
                topicName
        );
    }

    @Override
    public void updateTopicConfigs(String topicName, Map<String, String> configs) {
        ConfigResource resource = topicResource(topicName);
        Collection<AlterConfigOp> operations = configs.entrySet().stream()
                .map(entry -> new AlterConfigOp(
                        new ConfigEntry(entry.getKey(), entry.getValue()),
                        AlterConfigOp.OpType.SET
                ))
                .toList();
        await(admin.incrementalAlterConfigs(Map.of(resource, operations)).all(), topicName);
    }

    private ConfigResource topicResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    private <T> T await(KafkaFuture<T> future, String topicName) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException exception) {
            throw new KafkaUnavailableException("Kafka no respondió dentro del tiempo configurado", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("La operación Kafka fue interrumpida", exception);
        } catch (ExecutionException exception) {
            throw translate(exception.getCause(), topicName);
        }
    }

    private RuntimeException translate(Throwable cause, String topicName) {
        if (cause instanceof UnknownTopicOrPartitionException) {
            return new TopicNotFoundException(topicName);
        }
        if (cause instanceof TopicExistsException) {
            return new TopicConflictException("El tópico ya existe: " + topicName);
        }
        if (cause instanceof InvalidPartitionsException) {
            return new TopicConflictException("Cantidad de particiones incompatible para el tópico");
        }
        if (cause instanceof InvalidReplicationFactorException || cause instanceof InvalidConfigurationException) {
            return new InvalidAdminRequestException("Kafka rechazó los parámetros administrativos");
        }
        if (cause instanceof TimeoutException
                || cause instanceof BrokerNotAvailableException
                || cause instanceof AuthenticationException
                || cause instanceof AuthorizationException) {
            return new KafkaUnavailableException("Kafka no está disponible para la operación solicitada", cause);
        }
        return new KafkaAdminOperationException("La operación administrativa de Kafka falló", cause);
    }
}
