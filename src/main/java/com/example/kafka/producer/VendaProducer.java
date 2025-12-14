package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class VendaProducer {

    private final KafkaTemplate<String, String> kafka;

    public VendaProducer(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    public void send(String key, String payload) {
        // send asynchronously; KafkaTemplate is configured with idempotence (exactly-once semantics)
        kafka.send("vendas", key, payload);
    }

    public void sendTransactional(String key, String payload) {
        // Note: This method requires TRANSACTIONAL_ID_CONFIG to be set in KafkaConfig
        // Currently disabled - use send() method which uses idempotence for safe delivery
        kafka.executeInTransaction(template -> {
            template.send("vendas", key, payload);
            return null;
        });
    }
}