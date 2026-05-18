package org.assignments.processing.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assignments.processing.dto.OrderEvent;
import org.assignments.processing.entity.OutboxEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
//@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String KAFKA_BOOTSTRAP_SERVER;

    @Value("${spring.kafka.consumer.group-id}")
    private String KAFKA_PROCESSING_CONSUMER_GROUP;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String KAFKA_GROUP_AUTO_OFFSET_RESET;

    @Value("${spring.kafka.consumer.retry_backoff_ms}")
    private String KAFKA_RETRY_BACKOFF_MILLI_SEC;



    /*
     Consumer Configuration
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_PROCESSING_CONSUMER_GROUP);

//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_GROUP_AUTO_OFFSET_RESET);
        config.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, KAFKA_RETRY_BACKOFF_MILLI_SEC);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,    10);
        JacksonJsonDeserializer<String> payloadJsonDeserializer = new JacksonJsonDeserializer<>();
        payloadJsonDeserializer.addTrustedPackages("org.assignments.processing.dto");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), payloadJsonDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // Crucial step: Set AckMode to MANUAL or MANUAL_IMMEDIATE
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setConcurrency(3);   // 3 consumer threads per topic

        return factory;
    }

}
