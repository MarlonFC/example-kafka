package com.example.kafka.producer;

import com.example.kafka.dto.VendaRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Serviço responsável por enviar mensagens de venda para o Kafka.
 *
 * Utiliza configuração com idempotência habilitada para garantir exactly-once delivery.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Service
public class VendaProducer {

    private static final Logger log = LoggerFactory.getLogger(VendaProducer.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final String topicName;

    public VendaProducer(KafkaTemplate<String, String> kafka,
                        ObjectMapper objectMapper,
                        @Value("${app.kafka.topic.vendas:vendas}") String topicName) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    /**
     * Envia uma venda para processamento via Kafka.
     *
     * @param key Chave para particionamento
     * @param vendaRequest Dados da venda
     * @throws RuntimeException em caso de erro na serialização
     */
    public void send(String key, VendaRequest vendaRequest) {
        try {
            String payload = objectMapper.writeValueAsString(vendaRequest);

            log.debug("Enviando venda para Kafka - key={}, topic={}, pedidoId={}",
                     key, topicName, vendaRequest.getPedidoId());

            CompletableFuture<SendResult<String, String>> future = kafka.send(topicName, key, payload);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Venda enviada com sucesso - pedidoId={}, offset={}, partition={}",
                            vendaRequest.getPedidoId(),
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition());
                } else {
                    log.error("Falha ao enviar venda - pedidoId={}, key={}",
                             vendaRequest.getPedidoId(), key, ex);
                    throw new RuntimeException("Erro ao enviar mensagem para Kafka", ex);
                }
            });

        } catch (Exception e) {
            log.error("Erro na serialização da venda - pedidoId={}", vendaRequest.getPedidoId(), e);
            throw new RuntimeException("Erro ao serializar dados da venda", e);
        }
    }

    /**
     * Envia uma venda usando transação (requer configuração adicional).
     *
     * @param key Chave para particionamento
     * @param vendaRequest Dados da venda
     * @deprecated Método mantido para compatibilidade, use send() que já garante idempotência
     */
    @Deprecated
    public void sendTransactional(String key, VendaRequest vendaRequest) {
        log.warn("Método sendTransactional está deprecated. Use send() que já garante exactly-once delivery via idempotência.");

        try {
            String payload = objectMapper.writeValueAsString(vendaRequest);

            kafka.executeInTransaction(template -> {
                template.send(topicName, key, payload);
                return null;
            });

            log.info("Venda enviada via transação - pedidoId={}", vendaRequest.getPedidoId());

        } catch (Exception e) {
            log.error("Erro ao enviar venda via transação - pedidoId={}", vendaRequest.getPedidoId(), e);
            throw new RuntimeException("Erro ao enviar venda via transação", e);
        }
    }
}