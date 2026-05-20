package org.assignments.processing.config;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.client-id:processing-service-producer}")
    private String clientId;

    @Value("${spring.kafka.producer.acks:all}")
    private String ack;

    @Value("${spring.kafka.producer.transaction-id-prefix:processing-service-tx-}")
    private String transactionIdPrefix;

    @Value("${spring.kafka.producer.retries:3}")
    private int retries;

    @Value("${spring.kafka.producer.batch-size:16384}")
    private int batchSize;

    @Value("${spring.kafka.producer.linger-ms:5}")
    private int lingerMs;

    @Value("${spring.kafka.producer.buffer-memory:33554432}")
    private long bufferMemory;

    @Value("${spring.kafka.producer.request-timeout-ms:30000}")
    private int requestTimeoutMs;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms:120000}")
    private int deliveryTimeoutMs;

    @Value("${spring.kafka.producer.properties.enable.idempotence:true}")
    private String enableIdempotence;

    @Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection:1}")
    private int maxInFlightRequestsPerConnection;

    @Value("${spring.kafka.producer.properties.retry.backoff.ms:500}")
    private int retryBackoffMs;



// =========================================================
    // Producer Factory
    // =========================================================

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = producerConfigs();
        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);

        // Set transactional ID prefix for exactly-once semantics
        //factory.setTransactionIdPrefix(transactionIdPrefix);

        return factory;
    }

// Kafka Template
    // =========================================================

    /**
     * KafkaTemplate — primary bean used by OutboxPoller to send messages.
     * Transactional by default (uses producerFactory with transactionIdPrefix).
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory) {

        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic("saga.result");   // fallback topic (rarely used directly)
        return template;
    }

    // =========================================================
    // Kafka Transaction Manager
    // =========================================================

    /**
     * KafkaTransactionManager — used when you need to coordinate
     * a Kafka publish with a DB transaction (exactly-once).
     *
     * Usage in OutboxPoller:
     *   @Transactional("kafkaTransactionManager")
     *   public void pollAndPublish() { ... }
     *
     * This ensures: if Kafka send succeeds but DB update fails → rollback both.
     */
//    @Bean
//    public KafkaTransactionManager<String, String> kafkaTransactionManager(
//            ProducerFactory<String, String> producerFactory
//    ) {
//        log.info("creating kafkaTransactionManager");
//        return new KafkaTransactionManager<>(producerFactory);
//    }

    @Bean
    //@Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }


    // =========================================================
    // Producer Config Properties
    // =========================================================

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        log.info("KAFKA_BOOTSTRAP_SERVER {}", bootstrapServers);
        // ── Connection ────────────────────────────────────────
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,          clientId);


        // 2026-05-14 16:51:58.339 [scheduling-1] INFO  o.a.k.clients.producer.KafkaProducer - [Producer clientId=order-service-producer-1] Instantiated an idempotent producer.

        // ── Serializers ───────────────────────────────────────
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // ── Reliability ───────────────────────────────────────
        // acks=all: leader + all in-sync replicas must acknowledge
        props.put(ProducerConfig.ACKS_CONFIG, ack);

        // Idempotence: exactly-once in a single producer session (no duplicates on retry)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);

        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);

        // max.in.flight = 1 with idempotence ensures strict ordering per partition
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);

        // ── Retry ─────────────────────────────────────────────
        props.put(ProducerConfig.RETRIES_CONFIG,              retries);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,     retryBackoffMs);     // wait 500ms between retries
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,   requestTimeoutMs);
         props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,  deliveryTimeoutMs);

        // ── Batching & Throughput ─────────────────────────────
        // batch.size: accumulate up to 16KB before sending
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,    batchSize);

        // linger.ms: wait up to 5ms for more records to batch together
        props.put(ProducerConfig.LINGER_MS_CONFIG,     lingerMs);

        // buffer.memory: total memory for unsent messages (32MB)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);

        // ── Compression ───────────────────────────────────────
        // snappy: good balance of speed and compression ratio
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return props;
    }

    @Bean
    public NewTopic SagaResultTopic(){
        log.info("creating the topic saga.result");
        return TopicBuilder.name("saga.result").partitions(3).replicas(1).build();
    }


}
