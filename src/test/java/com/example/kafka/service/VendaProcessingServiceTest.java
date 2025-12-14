package com.example.kafka.service;

import com.example.kafka.dto.VendaRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para VendaProcessingService.
 *
 * Exemplifica:
 * - Testes de validação de regras de negócio
 * - Testes de exceções customizadas
 * - Uso de nested classes para organizar cenários
 * - Testes parametrizados
 * - AssertJ para assertions mais expressivas
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Testes para VendaProcessingService")
class VendaProcessingServiceTest {

    private VendaProcessingService vendaProcessingService;

    @BeforeEach
    void setUp() {
        vendaProcessingService = new VendaProcessingService();
    }

    @Nested
    @DisplayName("Testes de processamento válido")
    class ProcessamentoValidoTests {

        @Test
        @DisplayName("Deve processar venda com dados válidos")
        void deveProcessarVendaComDadosValidos() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String orderId = "order-123";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }

        @Test
        @DisplayName("Deve processar venda com valor máximo permitido")
        void deveProcessarVendaComValorMaximo() {
            // Arrange
            VendaRequest vendaRequest = new VendaRequest();
            vendaRequest.setPedidoId(1L);
            vendaRequest.setProduto("Produto Caro");
            vendaRequest.setValor(new BigDecimal("100000.00"));
            vendaRequest.setQuantidade(1);
            vendaRequest.setEmailCliente("cliente@email.com");

            String orderId = "order-max";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }

        @Test
        @DisplayName("Deve processar venda com valor mínimo válido")
        void deveProcessarVendaComValorMinimo() {
            // Arrange
            VendaRequest vendaRequest = new VendaRequest();
            vendaRequest.setPedidoId(1L);
            vendaRequest.setProduto("Produto Barato");
            vendaRequest.setValor(new BigDecimal("0.01"));
            vendaRequest.setQuantidade(1);
            vendaRequest.setEmailCliente("cliente@email.com");

            String orderId = "order-min";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }
    }

    @Nested
    @DisplayName("Testes de validação - casos de erro")
    class ValidacaoErroTests {

        @Test
        @DisplayName("Deve falhar com valor zero")
        void deveFalharComValorZero() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setValor(BigDecimal.ZERO);

            String orderId = "order-zero";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Valor deve ser positivo");
        }

        @Test
        @DisplayName("Deve falhar com valor negativo")
        void deveFalharComValorNegativo() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setValor(new BigDecimal("-100.00"));

            String orderId = "order-negative";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Valor deve ser positivo");
        }

        @Test
        @DisplayName("Deve falhar com valor acima do limite")
        void deveFalharComValorAcimaLimite() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setValor(new BigDecimal("100000.01"));

            String orderId = "order-over-limit";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Valor excede o limite máximo permitido");
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1, -10})
        @DisplayName("Deve falhar com quantidade inválida")
        void deveFalharComQuantidadeInvalida(int quantidade) {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setQuantidade(quantidade);

            String orderId = "order-invalid-qty";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Quantidade deve ser positiva");
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "  ", "\t", "\n"})
        @DisplayName("Deve falhar com produto vazio ou em branco")
        void deveFalharComProdutoVazio(String produto) {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setProduto(produto);

            String orderId = "order-empty-product";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Produto é obrigatório");
        }

        @Test
        @DisplayName("Deve falhar com produto null")
        void deveFalharComProdutoNull() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setProduto(null);

            String orderId = "order-null-product";

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> vendaProcessingService.processVenda(vendaRequest, orderId)
            );

            assertThat(exception.getMessage()).isEqualTo("Produto é obrigatório");
        }
    }

    @Nested
    @DisplayName("Testes de casos extremos")
    class CasosExtremosTests {

        @Test
        @DisplayName("Deve processar venda com quantidade muito alta")
        void deveProcessarVendaComQuantidadeAlta() {
            // Arrange
            VendaRequest vendaRequest = new VendaRequest();
            vendaRequest.setPedidoId(1L);
            vendaRequest.setProduto("Produto em Massa");
            vendaRequest.setValor(new BigDecimal("1.00"));
            vendaRequest.setQuantidade(Integer.MAX_VALUE);
            vendaRequest.setEmailCliente("cliente@email.com");

            String orderId = "order-high-qty";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }

        @Test
        @DisplayName("Deve processar venda com produto com caracteres especiais")
        void deveProcessarVendaComProdutoCaracteresEspeciais() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setProduto("Produto com àçêñtos & símb@los #especiais!");

            String orderId = "order-special-chars";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }

        @Test
        @DisplayName("Deve processar venda com email complexo")
        void deveProcessarVendaComEmailComplexo() {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setEmailCliente("usuario.teste+tag@subdomain.exemplo-empresa.com.br");

            String orderId = "order-complex-email";

            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> vendaProcessingService.processVenda(vendaRequest, orderId));
        }
    }

    /**
     * Método auxiliar para criar uma venda válida padrão.
     * Facilita a criação de objetos de teste e reduz duplicação.
     */
    private VendaRequest criarVendaValida() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(12345L);
        venda.setProduto("Notebook Dell Inspiron");
        venda.setValor(new BigDecimal("2500.99"));
        venda.setQuantidade(2);
        venda.setEmailCliente("cliente@email.com");
        return venda;
    }
}
