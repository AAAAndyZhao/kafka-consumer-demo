package org.demo.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
@Slf4j
public class PooledKafkaConsumerFactory extends BasePooledObjectFactory<KafkaConsumer<String, String>> {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private String enableAutoCommit;
    @Value("${spring.kafka.consumer.max-poll-records}")
    private int maxPollRecords;

    @Override
    public KafkaConsumer<String, String> create() {
        log.info("Creating new KafkaConsumer: bootstrapServers={}, groupId={}", bootstrapServers, groupId);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        log.info("Subscribing to topics: slowTopic, commonTopic, fastTopic");
        consumer.subscribe(Arrays.asList("slowTopic", "commonTopic", "fastTopic"));
        return consumer;
    }

    @Override
    public PooledObject<KafkaConsumer<String, String>> wrap(KafkaConsumer<String, String> consumer) {
        return new DefaultPooledObject<>(consumer);
    }

    @Override
    public void destroyObject(PooledObject<KafkaConsumer<String, String>> pooledObject) {
        pooledObject.getObject().close();
    }

    /*
    @Description: Validate the KafkaConsumer object. Poll with a short timeout to check if the consumer is still functional.
     */
    @Override
    public boolean validateObject(PooledObject<KafkaConsumer<String, String>> pooledObject) {
        log.info("Validating KafkaConsumer: {}", pooledObject.getObject());
        KafkaConsumer<String, String> consumer = pooledObject.getObject();
        try {
            // Poll with a short timeout to check if consumer is still functional
            consumer.poll(Duration.ZERO);
            return true;
        } catch (Exception e) {
            // If an exception occurs, the consumer is considered invalid
            return false;
        }
    }

    @Override
    public void activateObject(PooledObject<KafkaConsumer<String, String>> pooledObject) {
        log.info("Activating KafkaConsumer: {}", pooledObject.getObject());
        // So far, no action is needed when activating the object
    }

    @Override
    public void passivateObject(PooledObject<KafkaConsumer<String, String>> pooledObject) {
        log.info("Passivating KafkaConsumer: {}", pooledObject.getObject());
        // So far, no action is needed when passivating the object
    }

}
