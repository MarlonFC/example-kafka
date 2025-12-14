package com.example.kafka.listener;

import com.example.kafka.dto.VendaRequest;
import com.example.kafka.service.IdempotencyService;
import com.example.kafka.service.VendaProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer responsável por processar mensagens de venda do Kafka.
 *
 * Implementa garantias de idempotência e acknowledgment manual para exactly-once processing.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Component
public class VendaConsumer {

    private static final Logger log = LoggerFactory.getLogger(VendaConsumer.class);

    private final IdempotencyService idempotencyService;
    private final VendaProcessingService vendaProcessingService;
    private final ObjectMapper objectMapper;

    public VendaConsumer(IdempotencyService idempotencyService,
                        VendaProcessingService vendaProcessingService,
                        ObjectMapper objectMapper) {
        this.idempotencyService = idempotencyService;
        this.vendaProcessingService = vendaProcessingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Processa mensagens do tópico 'vendas' com garantia de idempotência.
     *
     * @param record Registro da mensagem Kafka
     * @param ack Acknowledgment manual para controle de offset
     */
    @KafkaListener(topics = "${app.kafka.topic.vendas:vendas}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String key = record.key();
            String value = record.value();
            
            log.info("Recebida mensagem - key={}, offset={}, partition={}",
                    key, record.offset(), record.partition());

            // Parse do JSON para DTO
            VendaRequest vendaRequest = parseVendaRequest(value);
            if (vendaRequest == null) {
                log.error("Não foi possível fazer parse da mensagem, pulando - offset={}", record.offset());
                ack.acknowledge();
                return;
            }

            // Determina o ID para idempotência
            String orderId = determineOrderId(vendaRequest, key, record);

            // Verifica e marca para processamento idempotente
            if (!idempotencyService.canProcessAndMark(orderId)) {
                log.warn("Mensagem duplicada ignorada - orderId={}, offset={}, partition={}",
                        orderId, record.offset(), record.partition());
                ack.acknowledge();
                return;
            }

            // Processa a venda
            log.info("Processando nova venda - orderId={}, pedidoId={}", orderId, vendaRequest.getPedidoId());
            vendaProcessingService.processVenda(vendaRequest, orderId);

            // Confirma o offset apenas após sucesso
            ack.acknowledge();
            log.info("Venda processada com sucesso - orderId={}, pedidoId={}", orderId, vendaRequest.getPedidoId());

        } catch (Exception ex) {
            log.error("Erro ao processar mensagem - offset={}, partition={}", 
                    record.offset(), record.partition(), ex);
            throw ex; // Relança para ativação do error handler (retries + DLQ)
        }
    }

    /**
     * Faz o parse do JSON para VendaRequest.
     */
    private VendaRequest parseVendaRequest(String json) {
        try {
            return objectMapper.readValue(json, VendaRequest.class);
        } catch (Exception e) {
            log.error("Erro ao fazer parse do JSON: {}", json, e);
            return null;
        }
    }

    /**
     * Determina o ID para controle de idempotência.
     */
    private String determineOrderId(VendaRequest vendaRequest, String key, ConsumerRecord<String, String> record) {
        if (vendaRequest.getPedidoId() != null) {
            return vendaRequest.getPedidoId().toString();
        }

        if (key != null && !key.trim().isEmpty()) {
            log.warn("PedidoId não encontrado, usando key do Kafka: {}", key);
            return key;
        }

        String fallbackId = "unknown-" + record.offset() + "-" + record.partition();
        log.warn("Nem pedidoId nem key disponíveis, usando fallback: {}", fallbackId);
        return fallbackId;
    }
}