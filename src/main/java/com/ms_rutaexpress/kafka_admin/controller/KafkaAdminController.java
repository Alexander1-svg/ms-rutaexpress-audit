package com.ms_rutaexpress.kafka_admin.controller;

import com.ms_rutaexpress.kafka_admin.dto.ClusterInfoResponse;
import com.ms_rutaexpress.kafka_admin.dto.CreateTopicRequest;
import com.ms_rutaexpress.kafka_admin.dto.IncreasePartitionsRequest;
import com.ms_rutaexpress.kafka_admin.dto.TopicConfigsResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicDetailResponse;
import com.ms_rutaexpress.kafka_admin.dto.TopicListResponse;
import com.ms_rutaexpress.kafka_admin.dto.UpdateTopicConfigsRequest;
import com.ms_rutaexpress.kafka_admin.service.KafkaAdministrationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/v1/admin")
public class KafkaAdminController {

    private final KafkaAdministrationService service;

    public KafkaAdminController(KafkaAdministrationService service) {
        this.service = service;
    }

    @GetMapping("/cluster")
    public ClusterInfoResponse cluster() {
        return service.cluster();
    }

    @GetMapping("/topics")
    public TopicListResponse topics() {
        return service.listTopics();
    }

    @GetMapping("/topics/{topicName}")
    public TopicDetailResponse topic(@PathVariable String topicName) {
        return service.topic(topicName);
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicDetailResponse> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        TopicDetailResponse created = service.createTopic(request);
        return ResponseEntity.created(URI.create("/api/v1/admin/topics/" + created.name())).body(created);
    }

    @GetMapping("/topics/{topicName}/configs")
    public TopicConfigsResponse topicConfigs(@PathVariable String topicName) {
        return service.topicConfigs(topicName);
    }

    @PatchMapping("/topics/{topicName}/configs")
    public TopicConfigsResponse updateTopicConfigs(
            @PathVariable String topicName,
            @Valid @RequestBody UpdateTopicConfigsRequest request
    ) {
        return service.updateTopicConfigs(topicName, request);
    }

    @PatchMapping("/topics/{topicName}/partitions")
    public TopicDetailResponse increasePartitions(
            @PathVariable String topicName,
            @Valid @RequestBody IncreasePartitionsRequest request
    ) {
        return service.increasePartitions(topicName, request.totalPartitions());
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topicName) {
        service.deleteTopic(topicName);
        return ResponseEntity.noContent().build();
    }
}

