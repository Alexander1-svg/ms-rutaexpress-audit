package com.ms_rutaexpress.kafka_admin.admin;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Map;
import java.util.Set;

public interface KafkaAdminOperations {

    Set<String> listTopics();

    TopicDescription describeTopic(String topicName);

    Config describeTopicConfigs(String topicName);

    ClusterMetadata describeCluster();

    void createTopic(String topicName, int partitions, short replicationFactor, Map<String, String> configs);

    void deleteTopic(String topicName);

    void increasePartitions(String topicName, int totalPartitions);

    void updateTopicConfigs(String topicName, Map<String, String> configs);
}

