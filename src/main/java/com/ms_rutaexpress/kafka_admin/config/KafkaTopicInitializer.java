package com.ms_rutaexpress.kafka_admin.config;

import com.ms_rutaexpress.kafka_admin.admin.KafkaAdminOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
public class KafkaTopicInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicInitializer.class);

    private final KafkaAdminOperations adminOperations;
    private final KafkaAdminProperties properties;

    public KafkaTopicInitializer(KafkaAdminOperations adminOperations, KafkaAdminProperties properties) {
        this.adminOperations = adminOperations;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.writeEnabled()) {
            log.info("Inicialización automática de tópicos omitida (write-enabled=false)");
            return;
        }

        Set<String> existingTopics = adminOperations.listTopics();

        for (String topic : properties.analyticsTopics()) {
            if (!existingTopics.contains(topic)) {
                log.info("Creando tópico requerido por configuración: {} (particiones={}, replicación={}, retención={}ms)",
                        topic, properties.defaultPartitions(), properties.defaultReplicationFactor(), properties.defaultRetentionMs());
                adminOperations.createTopic(
                        topic,
                        properties.defaultPartitions(),
                        properties.defaultReplicationFactor(),
                        Map.of("retention.ms", String.valueOf(properties.defaultRetentionMs()))
                );
            }
        }
    }
}