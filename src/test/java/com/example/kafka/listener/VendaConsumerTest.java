package com.example.kafka.listener;

import com.example.kafka.dto.VendaRequest;
import com.example.kafka.service.IdempotencyService;
import com.example.kafka.service.VendaProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Testes unitários para VendaConsumer.
 *
 * Exemplifica:
 * - Testes de consumer Kafka com mock de dependências
 * - Testes de idempotência e acknowledgment
 * - Testes de parsing de JSON
 * - Testes de fallback e error handling
 * - Simulação de diferentes cenários de mensagem
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Testes para VendaConsumer")
class VendaConsumerTest {

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private VendaProcessingService vendaProcessingService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Acknowledgment acknowledgment;

    private VendaConsumer vendaConsumer;

    @BeforeEach
    void setUp() {
        vendaConsumer = new VendaConsumer(idempotencyService, vendaProcessingService, objectMapper);
    }

    @Nested
    @DisplayName("Testes de processamento bem-sucedido")
    class ProcessamentoBemsucedidoTests {

        @Test
        @DisplayName("Deve processar mensagem válida com sucesso")
        void deveProcessarMensagemValidaComSucesso() throws Exception {
            // Arrange
            String key = "cliente-123";
            String jsonValue = "{\"pedidoId\":12345,\"produto\":\"Notebook\"}";
            VendaRequest vendaRequest = criarVendaValida();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark("12345")).thenReturn(true);
            doNothing().when(vendaProcessingService).processVenda(vendaRequest, "12345");

            // Act
            assertDoesNotThrow(() -> vendaConsumer.listen(record, acknowledgment));

            // Assert
            verify(objectMapper, times(1)).readValue(jsonValue, VendaRequest.class);
            verify(idempotencyService, times(1)).canProcessAndMark("12345");
            verify(vendaProcessingService, times(1)).processVenda(vendaRequest, "12345");
            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Deve usar key do Kafka quando pedidoId é null")
        void deveUsarKeyDoKafkaQuandoPedidoIdENull() throws Exception {
            // Arrange
            String key = "fallback-key";
            String jsonValue = "{\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValidaSemPedidoId();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark(key)).thenReturn(true);
            doNothing().when(vendaProcessingService).processVenda(vendaRequest, key);

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert - Deve usar a key do Kafka como fallback
            verify(idempotencyService, times(1)).canProcessAndMark(key);
            verify(vendaProcessingService, times(1)).processVenda(vendaRequest, key);
            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Deve criar ID de fallback quando nem pedidoId nem key estão disponíveis")
        void deveCriarIdDeFallbackQuandoNemPedidoIdNemKeyDisponiveis() throws Exception {
            // Arrange
            String jsonValue = "{\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValidaSemPedidoId();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 1, 150L, null, jsonValue // key é null
            );

            String expectedFallbackId = "unknown-150-1";

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark(expectedFallbackId)).thenReturn(true);

            ArgumentCaptor<String> orderIdCaptor = ArgumentCaptor.forClass(String.class);

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert
            verify(idempotencyService, times(1)).canProcessAndMark(orderIdCaptor.capture());
            verify(vendaProcessingService, times(1)).processVenda(eq(vendaRequest), orderIdCaptor.capture());

            String capturedOrderId = orderIdCaptor.getValue();
            assertThat(capturedOrderId).isEqualTo(expectedFallbackId);
            verify(acknowledgment, times(1)).acknowledge();
        }
    }

    @Nested
    @DisplayName("Testes de idempotência")
    class IdempotenciaTests {

        @Test
        @DisplayName("Deve ignorar mensagem duplicada")
        void deveIgnorarMensagemDuplicada() throws Exception {
            // Arrange
            String key = "cliente-duplicado";
            String jsonValue = "{\"pedidoId\":99999,\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setPedidoId(99999L);

            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark("99999")).thenReturn(false); // Já processado

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert - Não deve processar, mas deve fazer acknowledge
            verify(objectMapper, times(1)).readValue(jsonValue, VendaRequest.class);
            verify(idempotencyService, times(1)).canProcessAndMark("99999");
            verify(vendaProcessingService, never()).processVenda(any(), any());
            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Deve processar mensagem quando idempotência permite")
        void deveProcessarMensagemQuandoIdempotenciaPermite() throws Exception {
            // Arrange
            String key = "cliente-novo";
            String jsonValue = "{\"pedidoId\":88888,\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setPedidoId(88888L);

            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 200L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark("88888")).thenReturn(true); // Novo

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert - Deve processar normalmente
            verify(idempotencyService, times(1)).canProcessAndMark("88888");
            verify(vendaProcessingService, times(1)).processVenda(vendaRequest, "88888");
            verify(acknowledgment, times(1)).acknowledge();
        }
    }

    @Nested
    @DisplayName("Testes de cenários de erro")
    class CenariosErroTests {

        @Test
        @DisplayName("Deve tratar JSON malformado graciosamente")
        void deveTratarJsonMalformadoGraciosamente() throws Exception {
            // Arrange
            String key = "cliente-json-ruim";
            String jsonInvalido = "{ json malformado }";
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonInvalido
            );

            when(objectMapper.readValue(jsonInvalido, VendaRequest.class))
                .thenThrow(new RuntimeException("JSON inválido"));

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert - Deve fazer acknowledge mesmo com JSON inválido
            verify(objectMapper, times(1)).readValue(jsonInvalido, VendaRequest.class);
            verify(idempotencyService, never()).canProcessAndMark(any());
            verify(vendaProcessingService, never()).processVenda(any(), any());
            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Deve relançar exceção de processamento")
        void deveRelancarExcecaoDeProcessamento() throws Exception {
            // Arrange
            String key = "cliente-erro-processamento";
            String jsonValue = "{\"pedidoId\":77777,\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setPedidoId(77777L);

            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark("77777")).thenReturn(true);
            doThrow(new RuntimeException("Erro no processamento"))
                .when(vendaProcessingService).processVenda(vendaRequest, "77777");

            // Act & Assert - Deve relançar a exceção para ativar retry/DLQ
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                vendaConsumer.listen(record, acknowledgment);
            });

            assertThat(exception.getMessage()).isEqualTo("Erro no processamento");

            // Verify que tentou processar mas não fez acknowledge
            verify(vendaProcessingService, times(1)).processVenda(vendaRequest, "77777");
            verify(acknowledgment, never()).acknowledge();
        }

        @Test
        @DisplayName("Deve relançar exceção de idempotência")
        void deveRelancarExcecaoDeIdempotencia() throws Exception {
            // Arrange
            String key = "cliente-erro-idempotencia";
            String jsonValue = "{\"pedidoId\":66666,\"produto\":\"Produto\"}";
            VendaRequest vendaRequest = criarVendaValida();
            vendaRequest.setPedidoId(66666L);

            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, key, jsonValue
            );

            when(objectMapper.readValue(jsonValue, VendaRequest.class)).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark("66666"))
                .thenThrow(new RuntimeException("Erro no serviço de idempotência"));

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                vendaConsumer.listen(record, acknowledgment);
            });

            assertThat(exception.getMessage()).isEqualTo("Erro no serviço de idempotência");
            verify(vendaProcessingService, never()).processVenda(any(), any());
            verify(acknowledgment, never()).acknowledge();
        }
    }

    @Nested
    @DisplayName("Testes de parsing e conversão")
    class ParsingConversaoTests {

        @Test
        @DisplayName("Deve fazer parse correto de JSON complexo")
        void deveFazerParseCorretoDeJsonComplexo() throws Exception {
            // Arrange
            String jsonComplexo = "{\n" +
                "  \"pedidoId\": 12345,\n" +
                "  \"produto\": \"Notebook Dell Inspiron 15\",\n" +
                "  \"valor\": 2500.99,\n" +
                "  \"quantidade\": 2,\n" +
                "  \"emailCliente\": \"cliente@empresa.com.br\"\n" +
                "}";

            VendaRequest vendaEsperada = criarVendaCompleta();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, "key", jsonComplexo
            );

            when(objectMapper.readValue(jsonComplexo, VendaRequest.class)).thenReturn(vendaEsperada);
            when(idempotencyService.canProcessAndMark("12345")).thenReturn(true);

            ArgumentCaptor<VendaRequest> vendaCaptor = ArgumentCaptor.forClass(VendaRequest.class);

            // Act
            vendaConsumer.listen(record, acknowledgment);

            // Assert
            verify(vendaProcessingService, times(1)).processVenda(vendaCaptor.capture(), eq("12345"));
            VendaRequest vendaCapturada = vendaCaptor.getValue();
            assertThat(vendaCapturada).isEqualTo(vendaEsperada);
        }

        @Test
        @DisplayName("Deve tratar caracteres especiais no JSON")
        void deveTratarCaracteresEspeciaisNoJson() throws Exception {
            // Arrange
            String jsonComEspeciais = "{\n" +
                "  \"pedidoId\": 54321,\n" +
                "  \"produto\": \"Produto com àçêñtôs & símb@los #especiais! 中文 🎉\",\n" +
                "  \"valor\": 1500.50,\n" +
                "  \"quantidade\": 1,\n" +
                "  \"emailCliente\": \"usuário.téste+tag@exêmplo.com.br\"\n" +
                "}";

            VendaRequest vendaComEspeciais = criarVendaComCaracteresEspeciais();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "vendas", 0, 100L, "key-especiais", jsonComEspeciais
            );

            when(objectMapper.readValue(jsonComEspeciais, VendaRequest.class)).thenReturn(vendaComEspeciais);
            when(idempotencyService.canProcessAndMark("54321")).thenReturn(true);

            // Act
            assertDoesNotThrow(() -> vendaConsumer.listen(record, acknowledgment));

            // Assert
            verify(objectMapper, times(1)).readValue(jsonComEspeciais, VendaRequest.class);
            verify(vendaProcessingService, times(1)).processVenda(vendaComEspeciais, "54321");
            verify(acknowledgment, times(1)).acknowledge();
        }
    }

    @Nested
    @DisplayName("Testes de diferentes cenários de Consumer Record")
    class ConsumerRecordTests {

        @Test
        @DisplayName("Deve processar record de diferentes partições")
        void deveProcessarRecordDeDiferentesParticoes() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            int[] particoes = {0, 1, 2};

            when(objectMapper.readValue(anyString(), eq(VendaRequest.class))).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark(anyString())).thenReturn(true);

            for (int particao : particoes) {
                // Arrange para cada partição
                ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    "vendas", particao, 100L + particao, "key-" + particao, "{\"pedidoId\":12345}"
                );

                // Act
                vendaConsumer.listen(record, acknowledgment);
            }

            // Assert
            verify(vendaProcessingService, times(particoes.length)).processVenda(any(), any());
            verify(acknowledgment, times(particoes.length)).acknowledge();
        }

        @Test
        @DisplayName("Deve processar records com diferentes offsets")
        void deveProcessarRecordsComDiferentesOffsets() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            long[] offsets = {100L, 250L, 500L, 1000L};

            when(objectMapper.readValue(anyString(), eq(VendaRequest.class))).thenReturn(vendaRequest);
            when(idempotencyService.canProcessAndMark(anyString())).thenReturn(true);

            for (long offset : offsets) {
                // Arrange para cada offset
                ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    "vendas", 0, offset, "key", "{\"pedidoId\":12345}"
                );

                // Act
                vendaConsumer.listen(record, acknowledgment);
            }

            // Assert
            verify(vendaProcessingService, times(offsets.length)).processVenda(any(), any());
            verify(acknowledgment, times(offsets.length)).acknowledge();
        }
    }

    /**
     * Métodos auxiliares para criar objetos de teste.
     */
    private VendaRequest criarVendaValida() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(12345L);
        venda.setProduto("Notebook Dell");
        venda.setValor(new BigDecimal("2500.99"));
        venda.setQuantidade(2);
        venda.setEmailCliente("cliente@email.com");
        return venda;
    }

    private VendaRequest criarVendaValidaSemPedidoId() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(null); // Sem pedidoId
        venda.setProduto("Produto Genérico");
        venda.setValor(new BigDecimal("100.00"));
        venda.setQuantidade(1);
        venda.setEmailCliente("cliente@email.com");
        return venda;
    }

    private VendaRequest criarVendaCompleta() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(12345L);
        venda.setProduto("Notebook Dell Inspiron 15");
        venda.setValor(new BigDecimal("2500.99"));
        venda.setQuantidade(2);
        venda.setEmailCliente("cliente@empresa.com.br");
        return venda;
    }

    private VendaRequest criarVendaComCaracteresEspeciais() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(54321L);
        venda.setProduto("Produto com àçêñtôs & símb@los #especiais! 中文 🎉");
        venda.setValor(new BigDecimal("1500.50"));
        venda.setQuantidade(1);
        venda.setEmailCliente("usuário.téste+tag@exêmplo.com.br");
        return venda;
    }
}
