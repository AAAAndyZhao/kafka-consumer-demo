package org.demo.kafkaconsumer.controller;

import jakarta.annotation.Resource;
import org.demo.kafkaconsumer.service.KafkaConsumerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class TaskController {
    // The topicMap is a map that maps the type of the message to the topic name.
    private final Map<String, String> topicMap = Map.of(
            "slow", "slowTopic",
            "common", "commonTopic",
            "fast", "fastTopic"
    );
    @Resource
    private KafkaConsumerService kafkaConsumerService;

    @GetMapping("/getContent")
    public List<String> getContent(@RequestParam("type") String type) {
        if (!topicMap.containsKey(type)) {
            return List.of("Invalid type");
        }
        return kafkaConsumerService.getMessage(topicMap.get(type));
    }

}
