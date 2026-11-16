package com.ms_rutaexpress.kafka_admin.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaAdminProperties(
        @NotEmpty List<@NotBlank String> bootstrapServers,
        @NotBlank String securityProtocol,
        @NotNull Duration requestTimeout,
        @Min(1) int defaultPartitions,
        @Min(1) short defaultReplicationFactor,
        boolean writeEnabled,
        @NotNull List<@NotBlank String> analyticsTopics,
        @NotNull List<@NotBlank String> protectedTopics
) {
    public KafkaAdminProperties {
        bootstrapServers = bootstrapServers == null ? List.of() : List.copyOf(bootstrapServers);
        analyticsTopics = analyticsTopics == null ? List.of() : List.copyOf(analyticsTopics);
        protectedTopics = protectedTopics == null ? List.of() : List.copyOf(protectedTopics);
    }
}

