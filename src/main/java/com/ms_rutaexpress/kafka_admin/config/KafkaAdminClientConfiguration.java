package com.ms_rutaexpress.kafka_admin.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminClientConfiguration {

    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaAdminProperties properties) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", properties.bootstrapServers())
        );
        configuration.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                properties.securityProtocol()
        );
        configuration.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Math.toIntExact(properties.requestTimeout().toMillis())
        );
        configuration.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Math.toIntExact(properties.requestTimeout().toMillis())
        );
        return Admin.create(configuration);
    }
}
