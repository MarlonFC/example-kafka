package com.example.kafka.controller;

import com.example.kafka.dto.VendaRequest;
import com.example.kafka.dto.VendaResponse;
import com.example.kafka.producer.VendaProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller responsável pelo recebimento de vendas via API REST.
 *
 * As vendas são enviadas assincronamente para processamento via Kafka.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Tag(name = "Vendas", description = "API para gerenciamento de vendas")
@RestController
@RequestMapping("/api/v1/vendas")
public class VendaController {

    private static final Logger log = LoggerFactory.getLogger(VendaController.class);
    private final VendaProducer producer;

    public VendaController(VendaProducer producer) {
        this.producer = producer;
    }

    @Operation(
        summary = "Criar nova venda",
        description = "Recebe os dados de uma venda e envia para processamento assíncrono via Kafka"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "202", description = "Venda aceita para processamento",
                     content = @Content(mediaType = "application/json",
                                      schema = @Schema(implementation = VendaResponse.class))),
        @ApiResponse(responseCode = "400", description = "Dados inválidos fornecidos"),
        @ApiResponse(responseCode = "500", description = "Erro interno do servidor")
    })
    @PostMapping
    public ResponseEntity<VendaResponse> criarVenda(
            @Parameter(description = "Chave personalizada para particionamento (opcional)",
                      example = "cliente-123")
            @RequestParam(required = false) String key,
            @Parameter(description = "Dados da venda", required = true)
            @Valid @RequestBody VendaRequest vendaRequest) {

        try {
            log.info("Recebendo nova venda: pedidoId={}, produto={}",
                    vendaRequest.getPedidoId(), vendaRequest.getProduto());

            // Usa o pedidoId como chave se nenhuma chave foi fornecida
            String kafkaKey = key != null ? key : vendaRequest.getPedidoId().toString();

            // Envia para o Kafka
            producer.send(kafkaKey, vendaRequest);

            VendaResponse response = new VendaResponse(
                "Venda enviada para processamento",
                vendaRequest.getPedidoId(),
                "ACCEPTED"
            );

            log.info("Venda enviada com sucesso: pedidoId={}", vendaRequest.getPedidoId());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);

        } catch (Exception e) {
            log.error("Erro ao processar venda: pedidoId={}", vendaRequest.getPedidoId(), e);

            VendaResponse response = new VendaResponse(
                "Erro ao processar venda: " + e.getMessage(),
                vendaRequest.getPedidoId(),
                "ERROR"
            );

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @Operation(summary = "Health check", description = "Verifica se o serviço está funcionando")
    @ApiResponse(responseCode = "200", description = "Serviço funcionando")
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Serviço de vendas funcionando!");
    }
}