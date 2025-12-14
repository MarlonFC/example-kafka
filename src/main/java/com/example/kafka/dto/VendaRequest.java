package com.example.kafka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * DTO para requisição de venda.
 * Representa os dados necessários para criar uma nova venda no sistema.
 */
@Schema(description = "Dados da venda")
public class VendaRequest {

    @Schema(description = "ID único do pedido", example = "12345")
    @JsonProperty("pedidoId")
    @NotNull(message = "ID do pedido é obrigatório")
    private Long pedidoId;

    @Schema(description = "Nome do produto", example = "Notebook Dell Inspiron")
    @NotBlank(message = "Nome do produto é obrigatório")
    private String produto;

    @Schema(description = "Valor da venda", example = "2500.99")
    @NotNull(message = "Valor é obrigatório")
    @Positive(message = "Valor deve ser positivo")
    private BigDecimal valor;

    @Schema(description = "Quantidade do produto", example = "2")
    @NotNull(message = "Quantidade é obrigatória")
    @Positive(message = "Quantidade deve ser positiva")
    private Integer quantidade;

    @Schema(description = "Email do cliente", example = "cliente@email.com")
    @NotBlank(message = "Email do cliente é obrigatório")
    private String emailCliente;

    public VendaRequest() {}

    public VendaRequest(Long pedidoId, String produto, BigDecimal valor, Integer quantidade, String emailCliente) {
        this.pedidoId = pedidoId;
        this.produto = produto;
        this.valor = valor;
        this.quantidade = quantidade;
        this.emailCliente = emailCliente;
    }

    // Getters and Setters
    public Long getPedidoId() {
        return pedidoId;
    }

    public void setPedidoId(Long pedidoId) {
        this.pedidoId = pedidoId;
    }

    public String getProduto() {
        return produto;
    }

    public void setProduto(String produto) {
        this.produto = produto;
    }

    public BigDecimal getValor() {
        return valor;
    }

    public void setValor(BigDecimal valor) {
        this.valor = valor;
    }

    public Integer getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(Integer quantidade) {
        this.quantidade = quantidade;
    }

    public String getEmailCliente() {
        return emailCliente;
    }

    public void setEmailCliente(String emailCliente) {
        this.emailCliente = emailCliente;
    }

    @Override
    public String toString() {
        return "VendaRequest{" +
                "pedidoId=" + pedidoId +
                ", produto='" + produto + '\'' +
                ", valor=" + valor +
                ", quantidade=" + quantidade +
                ", emailCliente='" + emailCliente + '\'' +
                '}';
    }
}
