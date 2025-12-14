package com.example.kafka.listener;

import com.example.kafka.service.IdempotencyService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class VendaConsumer {

    private static final Logger log = LoggerFactory.getLogger(VendaConsumer.class);
    private final IdempotencyService idempotencyService;

    public VendaConsumer(IdempotencyService idempotencyService) {
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "vendas", groupId = "app-consumer")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String key = record.key();
            String value = record.value();
            
            log.info("Recebido key={} value={} offset={} partition={}",
                    key, value, record.offset(), record.partition());

            // Extrai o pedidoId do JSON
            String orderId = idempotencyService.extractOrderId(value);
            
            // Se não conseguiu extrair do JSON, usa a key do Kafka como fallback
            if (orderId == null || orderId.isEmpty()) {
                orderId = key != null ? key : "unknown-" + record.offset();
                log.warn("Não foi possível extrair pedidoId do JSON, usando key/offset como identificador: {}", orderId);
            } else {
                log.info("PedidoId extraído do JSON: {}", orderId);
            }

            // Verifica idempotência: se já foi processado, apenas confirma o offset
            boolean canProcess = idempotencyService.canProcessAndMark(orderId);
            log.info("Pode processar? {} (pedidoId={})", canProcess, orderId);
            
            if (!canProcess) {
                log.warn("Mensagem duplicada ignorada - pedidoId={} já foi processado. " +
                        "Offset={}, Partition={}", orderId, record.offset(), record.partition());
                // Ainda confirma o offset para não reprocessar a mesma mensagem
                ack.acknowledge();
                return;
            }

            // Processa a mensagem apenas se for nova
            log.info("Processando nova mensagem - pedidoId={}", orderId);
            
            // TODO: Aqui você faria o processamento real da venda:
            // - Salvar no banco de dados
            // - Atualizar estoque
            // - Enviar email de confirmação
            // - etc.
            
            // Simulação de processamento
            processVenda(value, orderId);

            // Confirma o offset apenas após processamento bem-sucedido
            ack.acknowledge();
            log.info("Mensagem processada com sucesso - pedidoId={}", orderId);

        } catch (Exception ex) {
            log.error("Erro ao processar mensagem - offset={}, partition={}", 
                    record.offset(), record.partition(), ex);
            // Lança exceção para que o DefaultErrorHandler trate (retries e DLQ)
            throw ex;
        }
    }

    private void processVenda(String jsonPayload, String orderId) {
        // Simulação de processamento
        // Em produção, aqui você faria:
        // - Parse do JSON para objeto Venda
        // - Validações de negócio
        // - Persistência no banco
        // - Cálculos, atualizações, etc.
        
        log.debug("Processando venda - pedidoId={}, payload={}", orderId, jsonPayload);
    }
}