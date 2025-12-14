package com.example.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * DTO para resposta de operações de venda.
 */
@Schema(description = "Resposta da operação de venda")
@JsonIgnoreProperties(ignoreUnknown = true)
public class VendaResponse {

    @Schema(description = "Mensagem de status", example = "Venda enviada para processamento")
    private String message;

    @Schema(description = "ID do pedido processado", example = "12345")
    private Long pedidoId;

    @Schema(description = "Status da operação", example = "ACCEPTED")
    private String status;

    public VendaResponse() {}

    public VendaResponse(String message, Long pedidoId, String status) {
        this.message = message;
        this.pedidoId = pedidoId;
        this.status = status;
    }

    // Getters and Setters
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getPedidoId() {
        return pedidoId;
    }

    public void setPedidoId(Long pedidoId) {
        this.pedidoId = pedidoId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "VendaResponse{" +
                "message='" + message + '\'' +
                ", pedidoId=" + pedidoId +
                ", status='" + status + '\'' +
                '}';
    }
}
