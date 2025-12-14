package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Serviço de idempotência para garantir que mensagens com o mesmo pedidoId
 * sejam processadas apenas uma vez.
 * 
 * NOTA: Esta implementação usa um Set em memória. Para produção, considere
 * usar um banco de dados (Redis, PostgreSQL, etc.) para persistência e
 * compartilhamento entre múltiplas instâncias.
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    
    // Pattern para extrair pedidoId do JSON: "pedidoId":123 ou "pedidoId": 123 ou 'pedidoId':123
    // Suporta tanto aspas duplas quanto simples, com ou sem espaços
    private static final Pattern PEDIDO_ID_PATTERN = Pattern.compile(
        "[\"']pedidoId[\"']\\s*:\\s*(\\d+)", 
        Pattern.CASE_INSENSITIVE
    );
    
    // Em produção, substitua por um banco de dados compartilhado
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    /**
     * Extrai o pedidoId do JSON da mensagem usando regex
     * Alternativa sem depender de bibliotecas externas além do Spring Boot
     */
    public String extractOrderId(String jsonPayload) {
        if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
            log.warn("JSON payload está vazio ou null");
            return null;
        }
        
        try {
            // Remove espaços em branco para facilitar o match
            String cleanJson = jsonPayload.trim().replaceAll("\\s+", " ");
            log.debug("Tentando extrair pedidoId do JSON: {}", cleanJson);
            
            Matcher matcher = PEDIDO_ID_PATTERN.matcher(cleanJson);
            if (matcher.find()) {
                String extractedId = matcher.group(1);
                log.debug("PedidoId extraído com sucesso: {}", extractedId);
                return extractedId;
            } else {
                log.warn("Não foi possível encontrar 'pedidoId' no JSON: {}", cleanJson);
            }
        } catch (Exception e) {
            log.error("Erro ao extrair pedidoId do JSON: {}", jsonPayload, e);
        }
        return null;
    }

    /**
     * Verifica se o pedido já foi processado
     */
    public boolean isAlreadyProcessed(String orderId) {
        if (orderId == null) {
            return false;
        }
        return processedIds.contains(orderId);
    }

    /**
     * Marca um pedido como processado
     */
    public void markAsProcessed(String orderId) {
        if (orderId != null) {
            processedIds.add(orderId);
            log.debug("Pedido {} marcado como processado", orderId);
        }
    }

    /**
     * Verifica se pode processar (não foi processado antes) e marca como processado
     * @return true se pode processar (não foi processado antes), false caso contrário
     */
    public boolean canProcessAndMark(String orderId) {
        if (orderId == null) {
            log.warn("PedidoId é null - permitindo processamento (pode ser mensagem antiga)");
            return true; // Permite processar se não tiver pedidoId (mensagens antigas)
        }
        
        // Verifica se já existe ANTES de tentar adicionar (para log mais claro)
        boolean alreadyExists = processedIds.contains(orderId);
        
        if (alreadyExists) {
            log.warn("Pedido {} JÁ FOI PROCESSADO anteriormente! Total de pedidos processados: {}", 
                    orderId, processedIds.size());
            return false;
        }
        
        // Adiciona ao set (atomicamente)
        processedIds.add(orderId);
        log.info("Pedido {} é NOVO - marcando como processado. Total agora: {}", 
                orderId, processedIds.size());
        return true;
    }

    /**
     * Limpa o cache (útil para testes ou limpeza periódica)
     * Em produção, implementar TTL ou limpeza baseada em data
     */
    public void clear() {
        processedIds.clear();
        log.info("Cache de idempotência limpo");
    }

    /**
     * Retorna o número de pedidos processados (útil para monitoramento)
     */
    public int getProcessedCount() {
        return processedIds.size();
    }
}

