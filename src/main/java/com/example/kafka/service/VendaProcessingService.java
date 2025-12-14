package com.example.kafka.service;

import com.example.kafka.dto.VendaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Serviço responsável pelo processamento efetivo de vendas.
 *
 * Aqui ficam as regras de negócio para processamento de uma venda.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Service
public class VendaProcessingService {

    private static final Logger log = LoggerFactory.getLogger(VendaProcessingService.class);

    /**
     * Processa uma venda com todas as validações e regras de negócio.
     *
     * @param vendaRequest Dados da venda
     * @param orderId ID para controle de idempotência
     * @throws IllegalArgumentException se os dados forem inválidos
     * @throws RuntimeException se houver erro no processamento
     */
    public void processVenda(VendaRequest vendaRequest, String orderId) {
        log.info("Iniciando processamento da venda - orderId={}, pedidoId={}",
                orderId, vendaRequest.getPedidoId());

        try {
            // 1. Validações de negócio
            validateVenda(vendaRequest);

            // 2. Cálculo do valor total
            BigDecimal valorTotal = calcularValorTotal(vendaRequest);
            log.debug("Valor total calculado: {} para orderId={}", valorTotal, orderId);

            // 3. Simulação de processamento de negócio
            // Em produção, aqui você faria:
            // - Salvar a venda no banco de dados
            // - Atualizar estoque dos produtos
            // - Enviar notificações (email, SMS)
            // - Integrar com sistemas de pagamento
            // - Gerar faturas/notas fiscais
            // - etc.

            simulateBusinessLogic(vendaRequest, valorTotal);

            log.info("Venda processada com sucesso - orderId={}, pedidoId={}, valorTotal={}",
                    orderId, vendaRequest.getPedidoId(), valorTotal);

        } catch (Exception e) {
            log.error("Erro no processamento da venda - orderId={}, pedidoId={}",
                     orderId, vendaRequest.getPedidoId(), e);
            throw e;
        }
    }

    /**
     * Valida os dados de uma venda.
     */
    private void validateVenda(VendaRequest venda) {
        if (venda.getValor().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Valor deve ser positivo");
        }

        if (venda.getQuantidade() <= 0) {
            throw new IllegalArgumentException("Quantidade deve ser positiva");
        }

        if (venda.getProduto() == null || venda.getProduto().trim().isEmpty()) {
            throw new IllegalArgumentException("Produto é obrigatório");
        }

        // Validação de valor máximo para evitar fraudes
        BigDecimal valorMaximo = new BigDecimal("100000.00");
        if (venda.getValor().compareTo(valorMaximo) > 0) {
            throw new IllegalArgumentException("Valor excede o limite máximo permitido");
        }

        log.debug("Validações de negócio passaram para pedidoId={}", venda.getPedidoId());
    }

    /**
     * Calcula o valor total da venda.
     */
    private BigDecimal calcularValorTotal(VendaRequest venda) {
        return venda.getValor().multiply(BigDecimal.valueOf(venda.getQuantidade()));
    }

    /**
     * Simula a lógica de negócio do processamento.
     * Em produção, substitua por implementação real.
     */
    private void simulateBusinessLogic(VendaRequest venda, BigDecimal valorTotal) {
        // Simulação de delay de processamento
        try {
            Thread.sleep(100); // Simula processamento
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Processamento interrompido", e);
        }

        log.debug("Simulação de processamento concluída - Produto: {}, Quantidade: {}, Valor Total: {}",
                 venda.getProduto(), venda.getQuantidade(), valorTotal);

        // Aqui você implementaria:
        // - persistenceService.saveVenda(venda);
        // - stockService.updateStock(venda.getProduto(), venda.getQuantidade());
        // - notificationService.sendConfirmation(venda.getEmailCliente(), venda);
        // - paymentService.processPayment(valorTotal);
        // - etc.
    }
}
