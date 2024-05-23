package org.demo.kafkaconsumer.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;


// @Configuration
// @EnableKafka
/*
    Note: This class is not used in the project. It is a configuration class for auto-configuration of Kafka consumer.
    But the project uses a custom Kafka consumer pool implementation instead of the auto-configured Kafka consumer.
*/
// public class KafkaConsumerConfig {
//     private final ConsumerFactory<String, String> consumerFactory;
//     @Autowired
//     public KafkaConsumerConfig(ConsumerFactory<String, String> consumerFactory) {
//         this.consumerFactory = consumerFactory;
//     }
//
//
//     public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//         ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//         factory.setConsumerFactory(consumerFactory);
//         factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//         return factory;
//     }
// }
