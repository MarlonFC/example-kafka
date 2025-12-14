package com.example.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração do Apache Kafka para Producer e Consumer.
 *
 * Configurada para garantir exactly-once delivery através de:
 * - Producer: Idempotência habilitada + acks=all + retries
 * - Consumer: Acknowledgment manual + tratamento de erros com DLQ
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.retries.max:3}")
    private int maxRetries;

    @Value("${app.kafka.retries.backoff-ms:1000}")
    private long backoffMs;

    /**
     * Configura o ProducerFactory com garantias de exactly-once delivery.
     *
     * @return ProducerFactory configurado com idempotência
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Configuração básica
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Configurações para exactly-once delivery e alta confiabilidade
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Aguarda confirmação de todas as réplicas
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Tentativas ilimitadas
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Garante ordem

        // Configurações de performance e batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Aguarda 5ms para formar batch
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB

        // Timeouts
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000); // 30s
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000); // 10s

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Template para envio de mensagens Kafka.
     *
     * @param producerFactory Factory do producer
     * @return KafkaTemplate configurado
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Configura o ConsumerFactory com configurações otimizadas.
     *
     * @return ConsumerFactory configurado
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Configuração básica
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Configurações para exactly-once processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Configurações de performance
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Processa até 10 registros por poll
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Timeouts
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30s
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10s

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Configura o container factory para listeners com tratamento de erros.
     *
     * @param consumerFactory Factory do consumer
     * @param kafkaTemplate Template para DLQ
     * @return Container factory configurado
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Configuração de acknowledgment manual para exactly-once
        factory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);

        // Configuração do tratamento de erros com Dead Letter Queue
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            recoverer,
            new FixedBackOff(backoffMs, maxRetries)
        );

        factory.setCommonErrorHandler(errorHandler);

        // Configurações de concorrência
        factory.setConcurrency(1); // 1 thread por partição para garantir ordem

        return factory;
    }
}