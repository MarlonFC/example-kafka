package com.example.kafka.controller;

import com.example.kafka.dto.VendaRequest;
import com.example.kafka.dto.VendaResponse;
import com.example.kafka.producer.VendaProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Testes de integração para VendaController usando @WebMvcTest.
 *
 * Exemplifica:
 * - Testes de controllers com MockMvc
 * - Testes de validação de entrada
 * - Testes de serialização/deserialização JSON
 * - Uso de ArgumentCaptor para verificar argumentos
 * - Testes de diferentes cenários HTTP
 */
@WebMvcTest(VendaController.class)
@DisplayName("Testes para VendaController")
class VendaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private VendaProducer vendaProducer;

    @Nested
    @DisplayName("Testes do endpoint POST /api/v1/vendas")
    class PostVendasTests {

        @Test
        @DisplayName("Deve criar venda com dados válidos")
        void deveCriarVendaComDadosValidos() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Mock do producer não lança exceção
            doNothing().when(vendaProducer).send(anyString(), any(VendaRequest.class));

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isAccepted())
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.message").value("Venda enviada para processamento"))
                    .andExpect(jsonPath("$.pedidoId").value(12345))
                    .andExpect(jsonPath("$.status").value("ACCEPTED"));

            // Verifica se o producer foi chamado corretamente
            verify(vendaProducer, times(1)).send(eq("12345"), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve criar venda com chave customizada")
        void deveCriarVendaComChaveCustomizada() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String customKey = "cliente-especial-123";
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<VendaRequest> vendaCaptor = ArgumentCaptor.forClass(VendaRequest.class);

            doNothing().when(vendaProducer).send(keyCaptor.capture(), vendaCaptor.capture());

            // Act
            mockMvc.perform(post("/api/v1/vendas")
                    .param("key", customKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isAccepted());

            // Assert - Verifica que a chave customizada foi usada
            assertThat(keyCaptor.getValue()).isEqualTo(customKey);
            assertThat(vendaCaptor.getValue().getPedidoId()).isEqualTo(vendaRequest.getPedidoId());
        }

        @Test
        @DisplayName("Deve retornar erro quando producer falha")
        void deveRetornarErroQuandoProducerFalha() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            doThrow(new RuntimeException("Kafka indisponível"))
                    .when(vendaProducer).send(anyString(), any(VendaRequest.class));

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.message").value("Erro ao processar venda: Kafka indisponível"))
                    .andExpect(jsonPath("$.status").value("ERROR"));
        }
    }

    @Nested
    @DisplayName("Testes de validação de entrada")
    class ValidacaoEntradaTests {

        @Test
        @DisplayName("Deve rejeitar venda com pedidoId null")
        void deveRejeitarVendaComPedidoIdNull() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setPedidoId(null);
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isBadRequest());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar venda com produto em branco")
        void deveRejeitarVendaComProdutoEmBranco() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setProduto("");
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isBadRequest());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar venda com valor negativo")
        void deveRejeitarVendaComValorNegativo() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setValor(new BigDecimal("-100.00"));
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isBadRequest());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar venda com quantidade zero")
        void deveRejeitarVendaComQuantidadeZero() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setQuantidade(0);
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isBadRequest());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar venda com email em branco")
        void deveRejeitarVendaComEmailEmBranco() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setEmailCliente("");
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isBadRequest());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar requisição com JSON malformado")
        void deveRejeitarRequisicaoComJsonMalformado() throws Exception {
            // Arrange
            String jsonMalformado = "{ invalid json }";

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(jsonMalformado))
                    .andExpect(status().isBadRequest())
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.message").value("JSON malformado ou inválido. Verifique a estrutura da requisição."))
                    .andExpect(jsonPath("$.status").value("INVALID_JSON"))
                    .andExpect(jsonPath("$.pedidoId").isEmpty());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar diferentes tipos de JSON malformado")
        void deveRejeitarDiferentesTiposDeJsonMalformado() throws Exception {
            // Arrange - Diferentes tipos de JSON inválido
            String[] jsonsInvalidos = {
                "{ invalid json }",           // Aspas faltando
                "{\"pedidoId\": }",          // Valor faltando
                "{\"pedidoId\": 123,}",      // Vírgula extra
                "not json at all",           // Não é JSON
                "{\"pedidoId\": \"abc\"",    // Chave não fechada
                "null",                      // JSON null
                ""                           // String vazia
            };

            for (String jsonInvalido : jsonsInvalidos) {
                // Act & Assert - Todos devem retornar 400 Bad Request
                mockMvc.perform(post("/api/v1/vendas")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonInvalido))
                        .andExpect(status().isBadRequest())
                        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                        .andExpect(jsonPath("$.status").value("INVALID_JSON"));
            }

            // Verifica que o producer nunca foi chamado
            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar requisição sem Content-Type")
        void deveRejeitarRequisicaoSemContentType() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .content(requestJson))  // Sem .contentType()
                    .andExpect(status().isUnsupportedMediaType())
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.message").value("Content-Type não suportado. Use 'application/json' para esta API."))
                    .andExpect(jsonPath("$.status").value("UNSUPPORTED_MEDIA_TYPE"))
                    .andExpect(jsonPath("$.pedidoId").isEmpty());

            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve rejeitar diferentes Content-Types não suportados")
        void deveRejeitarDiferentesContentTypesNaoSuportados() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            // Diferentes Content-Types inválidos
            String[] contentTypesInvalidos = {
                "text/plain",
                "application/xml",
                "text/xml",
                "application/x-www-form-urlencoded",
                "multipart/form-data",
                "text/html"
            };

            for (String contentType : contentTypesInvalidos) {
                // Act & Assert - Todos devem retornar 415 Unsupported Media Type
                mockMvc.perform(post("/api/v1/vendas")
                        .contentType(contentType)
                        .content(requestJson))
                        .andExpect(status().isUnsupportedMediaType())
                        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                        .andExpect(jsonPath("$.status").value("UNSUPPORTED_MEDIA_TYPE"));
            }

            // Verifica que o producer nunca foi chamado
            verify(vendaProducer, never()).send(anyString(), any(VendaRequest.class));
        }
    }

    @Nested
    @DisplayName("Testes do endpoint GET /api/v1/vendas/health")
    class HealthCheckTests {

        @Test
        @DisplayName("Deve retornar status de saúde")
        void deveRetornarStatusDeSaude() throws Exception {
            mockMvc.perform(get("/api/v1/vendas/health"))
                    .andExpect(status().isOk())
                    .andExpect(content().string("Serviço de vendas funcionando!"));
        }

        @Test
        @DisplayName("Deve retornar health check com Content-Type texto")
        void deveRetornarHealthCheckComContentTypTexto() throws Exception {
            mockMvc.perform(get("/api/v1/vendas/health"))
                    .andExpect(status().isOk())
                    .andExpect(content().contentType("text/plain;charset=UTF-8"));
        }
    }

    @Nested
    @DisplayName("Testes de cenários extremos")
    class CenariosExtremosTests {

        @Test
        @DisplayName("Deve processar venda com valores muito grandes")
        void deveProcessarVendaComValoresMuitoGrandes() throws Exception {
            // Arrange
            VendaRequest vendaRequest = new VendaRequest();
            vendaRequest.setPedidoId(Long.MAX_VALUE);
            vendaRequest.setProduto("Produto Caro " + "x".repeat(1000)); // Nome longo
            vendaRequest.setValor(new BigDecimal("99999.99"));
            vendaRequest.setQuantidade(1000);
            vendaRequest.setEmailCliente("usuario.muito.longo.com.nome.gigante@dominio.muito.longo.exemplo.com");

            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            doNothing().when(vendaProducer).send(anyString(), any(VendaRequest.class));

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isAccepted());

            verify(vendaProducer, times(1)).send(anyString(), any(VendaRequest.class));
        }

        @Test
        @DisplayName("Deve processar venda com caracteres especiais")
        void deveProcessarVendaComCaracteresEspeciais() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setProduto("Produto com àçêñtös & símb@los #especiais! 中文 🎉");
            vendaRequest.setEmailCliente("usuário.téste+tag@exêmplo-empresa.com.br");

            String requestJson = objectMapper.writeValueAsString(vendaRequest);

            doNothing().when(vendaProducer).send(anyString(), any(VendaRequest.class));

            // Act & Assert
            mockMvc.perform(post("/api/v1/vendas")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                    .andExpect(status().isAccepted());

            verify(vendaProducer, times(1)).send(anyString(), any(VendaRequest.class));
        }
    }

    /**
     * Método auxiliar para criar uma venda válida padrão.
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
