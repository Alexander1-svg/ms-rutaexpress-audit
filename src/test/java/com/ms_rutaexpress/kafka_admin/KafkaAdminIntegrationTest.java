package com.ms_rutaexpress.kafka_admin;

import com.ms_rutaexpress.kafka_admin.dto.CreateTopicRequest;
import com.ms_rutaexpress.kafka_admin.dto.UpdateTopicConfigsRequest;
import com.ms_rutaexpress.kafka_admin.service.KafkaAdministrationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.kafka.request-timeout=10s",
        "app.kafka.default-partitions=1",
        "app.kafka.default-replication-factor=1",
        "app.kafka.write-enabled=true",
        "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN",
        "logging.level.state.change.logger=OFF"
})
@EmbeddedKafka(
        partitions = 1,
        topics = "admin.integration.bootstrap",
        bootstrapServersProperty = "app.kafka.bootstrap-servers"
)
@DirtiesContext
class KafkaAdminIntegrationTest {

    private static final String TOPIC = "rutaexpress.admin.integration.v1";

    @Autowired
    private KafkaAdministrationService service;

    @Test
    void managesTopicLifecycleUsingRealKafkaAdminClient() throws Exception {
        assertThat(service.cluster().brokers()).hasSize(1);

        var created = service.createTopic(new CreateTopicRequest(
                TOPIC,
                1,
                (short) 1,
                Map.of("retention.ms", "60000", "cleanup.policy", "delete")
        ));
        assertThat(created.name()).isEqualTo(TOPIC);
        assertThat(created.partitionCount()).isEqualTo(1);
        assertThat(service.listTopics().topics()).contains(TOPIC);

        var configs = service.updateTopicConfigs(
                TOPIC,
                new UpdateTopicConfigsRequest(Map.of(
                        "retention.ms", "120000",
                        "min.insync.replicas", "1"
                ))
        );
        assertThat(configs.configs())
                .filteredOn(config -> config.name().equals("retention.ms"))
                .extracting(config -> config.value())
                .contains("120000");

        var partitioned = service.increasePartitions(TOPIC, 2);
        assertThat(partitioned.partitionCount()).isEqualTo(2);

        assertThat(service.listTopics().topics()).contains(TOPIC);
    }
}
