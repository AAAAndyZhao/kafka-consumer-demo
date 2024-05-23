package org.demo.kafkaconsumer.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.demo.kafkaconsumer.pool.KafkaConsumerPool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class KafkaConsumerService {
    @Resource
    private KafkaConsumerPool kafkaConsumerPool;

    @Value("${spring.kafka.consumer.max-poll-records}")
    private int maxPollRecords;

    public List<String> getMessage(String topic) {
        log.info("getMessage: topic={} maxPollRecords={}", topic, maxPollRecords);
        List<String> messages = new ArrayList<>();
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = kafkaConsumerPool.borrowObject(topic);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            log.info("Records received: size={}", records.count());
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (var record : records.records(topic)) {
                messages.add(record.value());
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                );
            }
            log.info("Messages received: size={}", messages.size());
            log.info("Offset to commit: {}", offsets);

            if (!messages.isEmpty()) {
                log.info("Committing offset: {}", offsets.values()
                        .stream().mapToLong(OffsetAndMetadata::offset).max().orElse(-1));
                consumer.commitSync(offsets);
            }
        } catch (Exception e) {
            log.error("Error in getMessage", e);
        } finally {
            if (consumer != null) {
                kafkaConsumerPool.returnObject(consumer);
            }
        }
        return messages;
    }
}
