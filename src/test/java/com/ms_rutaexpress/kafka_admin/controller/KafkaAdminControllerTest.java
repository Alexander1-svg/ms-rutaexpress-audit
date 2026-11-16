package com.ms_rutaexpress.kafka_admin.controller;

import com.ms_rutaexpress.kafka_admin.config.ApplicationConfiguration;
import com.ms_rutaexpress.kafka_admin.dto.TopicDetailResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicListResponse;
import com.ms_rutaexpress.kafka_admin.exception.KafkaUnavailableException;
import com.ms_rutaexpress.kafka_admin.exception.TopicConflictException;
import com.ms_rutaexpress.kafka_admin.exception.TopicNotFoundException;
import com.ms_rutaexpress.kafka_admin.service.KafkaAdministrationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(KafkaAdminController.class)
@Import(ApplicationConfiguration.class)
class KafkaAdminControllerTest {

    @MockitoBean
    private KafkaAdministrationService service;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void listsTopics() throws Exception {
        when(service.listTopics()).thenReturn(new TopicListResponse(2, List.of("a", "b")));

        mockMvc.perform(get("/api/v1/admin/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.topics[0]").value("a"));
    }

    @Test
    void createsTopicWith201AndLocation() throws Exception {
        when(service.createTopic(org.mockito.ArgumentMatchers.any())).thenReturn(topic("new-topic", 3));

        mockMvc.perform(post("/api/v1/admin/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "new-topic",
                                  "partitions": 3,
                                  "replicationFactor": 1,
                                  "configs": {"retention.ms": "60000"}
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/admin/topics/new-topic"))
                .andExpect(jsonPath("$.name").value("new-topic"));
    }

    @Test
    void rejectsInvalidCreateRequest() throws Exception {
        mockMvc.perform(post("/api/v1/admin/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"", "partitions":0, "replicationFactor":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Solicitud inválida"));
    }

    @Test
    void returnsNotFoundConflictAndUnavailableStatuses() throws Exception {
        when(service.topic("missing")).thenThrow(new TopicNotFoundException("missing"));
        doThrow(new TopicConflictException("Tópico protegido"))
                .when(service).deleteTopic("protected");
        when(service.listTopics()).thenThrow(new KafkaUnavailableException("timeout", new RuntimeException()));

        mockMvc.perform(get("/api/v1/admin/topics/missing"))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/v1/admin/topics/protected"))
                .andExpect(status().isConflict());
        mockMvc.perform(get("/api/v1/admin/topics"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value("Kafka no está disponible"));
    }

    @Test
    void updatesConfigsAndPartitionsAndDeletesTopic() throws Exception {
        when(service.updateTopicConfigs(
                org.mockito.ArgumentMatchers.eq("test-topic"),
                org.mockito.ArgumentMatchers.any()
        )).thenReturn(new com.ms_rutaexpress.kafka_admin.dto.TopicConfigsResponse("test-topic", List.of()));
        when(service.increasePartitions("test-topic", 4)).thenReturn(topic("test-topic", 4));
        doNothing().when(service).deleteTopic("test-topic");

        mockMvc.perform(patch("/api/v1/admin/topics/test-topic/configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configs\":{\"cleanup.policy\":\"delete\"}}"))
                .andExpect(status().isOk());
        mockMvc.perform(patch("/api/v1/admin/topics/test-topic/partitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"totalPartitions\":4}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitionCount").value(4));
        mockMvc.perform(delete("/api/v1/admin/topics/test-topic"))
                .andExpect(status().isNoContent());

        verify(service).deleteTopic("test-topic");
    }

    private TopicDetailResponse topic(String name, int partitions) {
        return new TopicDetailResponse(name, false, false, partitions, List.of(1), List.of());
    }
}
