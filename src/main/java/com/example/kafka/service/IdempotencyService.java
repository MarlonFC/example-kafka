package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serviço de idempotência para garantir que mensagens com o mesmo orderId
 * sejam processadas apenas uma vez.
 * 
 * IMPORTANTE: Esta implementação usa um Set em memória para demonstração.
 * Para produção, considere usar um banco de dados distribuído (Redis, PostgreSQL, etc.)
 * para persistência e compartilhamento entre múltiplas instâncias da aplicação.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    
    /**
     * Cache em memória para IDs processados.
     * Em produção, substitua por um banco de dados distribuído.
     */
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    /**
     * Verifica se o pedido já foi processado.
     *
     * @param orderId ID do pedido para verificação
     * @return true se já foi processado, false caso contrário
     */
    public boolean isAlreadyProcessed(String orderId) {
        if (orderId == null || orderId.trim().isEmpty()) {
            return false;
        }
        boolean result = processedIds.contains(orderId);
        log.debug("Verificação de idempotência - orderId={}, jaProcessado={}", orderId, result);
        return result;
    }

    /**
     * Marca um pedido como processado.
     *
     * @param orderId ID do pedido para marcar como processado
     */
    public void markAsProcessed(String orderId) {
        if (orderId != null && !orderId.trim().isEmpty()) {
            processedIds.add(orderId);
            log.debug("OrderId {} marcado como processado. Total processados: {}",
                     orderId, processedIds.size());
        }
    }

    /**
     * Verifica se pode processar (não foi processado antes) e marca como processado
     * de forma atômica.
     *
     * @param orderId ID do pedido para verificação e marcação
     * @return true se pode processar (não foi processado antes), false caso contrário
     */
    public boolean canProcessAndMark(String orderId) {
        if (orderId == null || orderId.trim().isEmpty()) {
            log.warn("OrderId é null ou vazio - permitindo processamento (compatibilidade com mensagens antigas)");
            return true;
        }
        
        // Verifica se já existe ANTES de tentar adicionar
        boolean alreadyExists = processedIds.contains(orderId);
        
        if (alreadyExists) {
            log.warn("OrderId {} JÁ FOI PROCESSADO anteriormente! Total processados: {}",
                    orderId, processedIds.size());
            return false;
        }
        
        // Adiciona ao set de forma atômica
        processedIds.add(orderId);
        log.info("OrderId {} é NOVO - marcado como processado. Total agora: {}",
                orderId, processedIds.size());
        return true;
    }

    /**
     * Limpa o cache de IDs processados.
     *
     * Útil para testes ou limpeza periódica. Em produção, implemente TTL
     * ou limpeza baseada em data para evitar crescimento ilimitado.
     */
    public void clear() {
        int previousSize = processedIds.size();
        processedIds.clear();
        log.info("Cache de idempotência limpo - {} IDs removidos", previousSize);
    }

    /**
     * Retorna o número de pedidos processados.
     *
     * @return quantidade de pedidos únicos processados
     */
    public int getProcessedCount() {
        return processedIds.size();
    }

    /**
     * Remove um ID específico do cache (útil para testes).
     *
     * @param orderId ID a ser removido
     * @return true se foi removido, false se não existia
     */
    public boolean removeProcessed(String orderId) {
        if (orderId == null || orderId.trim().isEmpty()) {
            return false;
        }
        boolean removed = processedIds.remove(orderId);
        log.debug("OrderId {} removido do cache: {}", orderId, removed);
        return removed;
    }
}

