package com.ms_rutaexpress.kafka_admin.service;

import com.ms_rutaexpress.kafka_admin.exception.InvalidAdminRequestException;
import com.ms_rutaexpress.kafka_admin.validation.TopicConfigValidator;
import com.ms_rutaexpress.kafka_admin.validation.TopicNameValidator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicValidatorsTest {

    private final TopicNameValidator nameValidator = new TopicNameValidator();
    private final TopicConfigValidator configValidator = new TopicConfigValidator();

    @Test
    void validatesTopicNames() {
        assertThatCode(() -> nameValidator.validateForCreation("rutaexpress.shipments.events.v1"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> nameValidator.validate("invalid topic"))
                .isInstanceOf(InvalidAdminRequestException.class);
        assertThatThrownBy(() -> nameValidator.validateForCreation("__internal"))
                .isInstanceOf(InvalidAdminRequestException.class);
    }

    @Test
    void validatesMutableConfigValues() {
        assertThatCode(() -> configValidator.validate(Map.of(
                "retention.ms", "-1",
                "retention.bytes", "1048576",
                "cleanup.policy", "delete,compact",
                "min.insync.replicas", "1"
        ))).doesNotThrowAnyException();

        assertThatThrownBy(() -> configValidator.validate(Map.of("retention.ms", "-2")))
                .isInstanceOf(InvalidAdminRequestException.class);
        assertThatThrownBy(() -> configValidator.validate(Map.of("cleanup.policy", "archive")))
                .isInstanceOf(InvalidAdminRequestException.class);
    }
}
