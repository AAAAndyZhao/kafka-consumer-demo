package org.demo.kafkaconsumer.pool;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.demo.kafkaconsumer.config.PooledKafkaConsumerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class KafkaConsumerPool {
    private final int CONSUMER_POOL_SIZE = 10;

    private final GenericObjectPool<KafkaConsumer<String, String>> consumerPool;

    @Autowired
    public KafkaConsumerPool(PooledKafkaConsumerFactory consumerFactory) {
        GenericObjectPoolConfig<KafkaConsumer<String, String>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(CONSUMER_POOL_SIZE);
        config.setMinIdle(2); // minimum number of idle objects in the pool
        config.setMaxIdle(5); // maximum number of idle objects in the pool
        config.setBlockWhenExhausted(true); // is block when the pool is exhausted
        config.setMaxWait(Duration.ofMillis(3000)); // max wait time for borrow object
        this.consumerPool = new GenericObjectPool<>(consumerFactory, config);
        // Initialize pool with minIdle consumers
        initializePoolWithMinIdleConsumers();
        log.info("Kafka consumer pool created with maxTotal={}, minIdle={}, maxIdle={}",
                config.getMaxTotal(), config.getMinIdle(), config.getMaxIdle());
        logPoolStatus();
    }

    private void initializePoolWithMinIdleConsumers() {
        for (int i = 0; i < consumerPool.getMinIdle(); i++) {
            try {
                consumerPool.addObject();
            } catch (Exception e) {
                log.error("Error adding object to pool", e);
            }
        }
        log.info("Initialized pool with {} idle consumers", consumerPool.getMinIdle());
    }

    /*
    @Description: Borrow a KafkaConsumer object from the pool.
        Call resetOffsets() to reset the offsets of the consumer to the last committed offset for each partition.
        Call resume() on the consumer to resume the paused consumption of messages.
     */
    public KafkaConsumer<String, String> borrowObject(String topic) {
        log.info("Borrowing KafkaConsumer from pool");
        logPoolStatus();
        try {
            KafkaConsumer<String, String> consumer = consumerPool.borrowObject();
            log.info("Borrowed KafkaConsumer from pool: {}", consumer);
            resetOffsets(consumer, topic);
            consumer.resume(consumer.assignment());
            return consumer;
        } catch (Exception e) {
            log.error("Failed to borrow KafkaConsumer from pool", e);
            throw new RuntimeException("Failed to borrow KafkaConsumer from pool", e);
        }
    }
    /*
    @Description: Return the KafkaConsumer object to the pool.
        Call pause() on the consumer to pause the consumption of messages but still keep the assignment of partitions.
        Can prevent rebalancing of partitions when the consumer is returned to the pool.
    */
    public void returnObject(KafkaConsumer<String, String> consumer) {
        log.info("Returning KafkaConsumer to pool: {}", consumer);
        try {
            consumer.pause(consumer.assignment());
            consumerPool.returnObject(consumer);
            log.info("Returned KafkaConsumer to pool: {}", consumer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to return KafkaConsumer to pool", e);
        }
    }

    public void logPoolStatus() {
        log.info("NumActive: {}, NumIdle: {}", consumerPool.getNumActive(), consumerPool.getNumIdle());
    }

    /*
    @Description: Reset the offsets of the consumer to the last committed offset for each partition.
        If no committed offset is found, reset to the beginning of the partition.
     */
    private void resetOffsets(KafkaConsumer<String, String> consumer, String topic) {
        log.info("Resetting offsets for consumer: {}", consumer);
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(Duration.ZERO);
        Set<TopicPartition> partitions = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(partitions);

        for (TopicPartition partition : partitions) {
            OffsetAndMetadata committed = committedOffsets.get(partition);
            if (committed != null) {
                consumer.seek(partition, committed.offset());
                log.info("Offset for partition {} reset to last committed offset {}", partition, committed.offset());
            } else {
                consumer.seekToBeginning(Collections.singleton(partition));
                log.info("No committed offset for partition {}, reset to beginning", partition);
            }
        }
    }
}
