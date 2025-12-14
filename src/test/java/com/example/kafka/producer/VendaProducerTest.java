package com.example.kafka.producer;

import com.example.kafka.dto.VendaRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Testes unitários para VendaProducer.
 *
 * Exemplifica:
 * - Mock de KafkaTemplate
 * - Testes de serialização JSON
 * - Testes de callbacks assíncronos
 * - Captura de argumentos para validação
 * - Simulação de cenários de erro
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Testes para VendaProducer")
class VendaProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    private VendaProducer vendaProducer;

    private final String topicName = "vendas-test";

    @BeforeEach
    void setUp() {
        vendaProducer = new VendaProducer(kafkaTemplate, objectMapper, topicName);
    }

    @Nested
    @DisplayName("Testes de envio bem-sucedido")
    class EnvioBemsucedidoTests {

        @Test
        @DisplayName("Deve enviar venda com sucesso")
        void deveEnviarVendaComSucesso() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "cliente-123";
            String jsonPayload = "{\"pedidoId\":12345}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);

            // Mock da resposta bem-sucedida do Kafka - usando mock simples
            SendResult<String, String> sendResult = mock(SendResult.class);
            CompletableFuture<SendResult<String, String>> future =
                CompletableFuture.completedFuture(sendResult);

            when(kafkaTemplate.send(topicName, key, jsonPayload)).thenReturn(future);

            // Act
            assertDoesNotThrow(() -> vendaProducer.send(key, vendaRequest));

            // Assert
            verify(objectMapper, times(1)).writeValueAsString(vendaRequest);
            verify(kafkaTemplate, times(1)).send(topicName, key, jsonPayload);
        }

        @Test
        @DisplayName("Deve usar topic correto configurado")
        void deveUsarTopicCorretoConfigurado() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "test-key";
            String jsonPayload = "{}";

            when(objectMapper.writeValueAsString(any())).thenReturn(jsonPayload);

            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);

            CompletableFuture<SendResult<String, String>> future =
                CompletableFuture.completedFuture(mock(SendResult.class));

            when(kafkaTemplate.send(topicCaptor.capture(), keyCaptor.capture(), payloadCaptor.capture()))
                .thenReturn(future);

            // Act
            vendaProducer.send(key, vendaRequest);

            // Assert
            assertThat(topicCaptor.getValue()).isEqualTo(topicName);
            assertThat(keyCaptor.getValue()).isEqualTo(key);
            assertThat(payloadCaptor.getValue()).isEqualTo(jsonPayload);
        }

        @Test
        @DisplayName("Deve serializar objeto corretamente")
        void deveSerializarObjetoCorretamente() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String expectedJson = "{\"pedidoId\":12345,\"produto\":\"Notebook\"}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(expectedJson);
            when(kafkaTemplate.send(any(String.class), any(String.class), any(String.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(mock(SendResult.class)));

            // Act
            vendaProducer.send("key", vendaRequest);

            // Assert - Verifica que o objeto foi passado para serialização
            verify(objectMapper, times(1)).writeValueAsString(vendaRequest);
        }
    }

    @Nested
    @DisplayName("Testes de cenários de erro")
    class CenariosErroTests {

        @Test
        @DisplayName("Deve lançar exceção em erro de serialização")
        void deveLancarExcecaoEmErroSerializacao() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "test-key";

            when(objectMapper.writeValueAsString(vendaRequest))
                .thenThrow(new RuntimeException("Erro de serialização"));

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                vendaProducer.send(key, vendaRequest);
            });

            assertThat(exception.getMessage()).isEqualTo("Erro ao serializar dados da venda");
            assertThat(exception.getCause().getMessage()).isEqualTo("Erro de serialização");

            // Verifica que o Kafka não foi chamado
            verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        }

        @Test
        @DisplayName("Deve tratar erro de envio para o Kafka")
        void deveTratarErroDeEnvioParaKafka() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "test-key";
            String jsonPayload = "{}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);

            // Simula falha no envio
            CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Kafka indisponível"));

            when(kafkaTemplate.send(eq(topicName), eq(key), eq(jsonPayload))).thenReturn(failedFuture);

            // Act - O método não deve lançar exceção diretamente, mas o callback sim
            assertDoesNotThrow(() -> vendaProducer.send(key, vendaRequest));

            // Assert
            verify(objectMapper, times(1)).writeValueAsString(vendaRequest);
            verify(kafkaTemplate, times(1)).send(topicName, key, jsonPayload);
        }
    }

    @Nested
    @DisplayName("Testes do método deprecated sendTransactional")
    class SendTransactionalTests {

        @Test
        @DisplayName("Deve enviar via transação")
        void deveEnviarViaTransacao() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "transactional-key";
            String jsonPayload = "{\"test\":\"value\"}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);
            when(kafkaTemplate.executeInTransaction(any())).thenReturn(null);

            // Act
            assertDoesNotThrow(() -> vendaProducer.sendTransactional(key, vendaRequest));

            // Assert
            verify(objectMapper, times(1)).writeValueAsString(vendaRequest);
            verify(kafkaTemplate, times(1)).executeInTransaction(any());
        }

        @Test
        @DisplayName("Deve tratar erro na transação")
        void deveTratarErroNaTransacao() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "transactional-key";
            String jsonPayload = "{}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);
            when(kafkaTemplate.executeInTransaction(any()))
                .thenThrow(new RuntimeException("Transação falhou"));

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                vendaProducer.sendTransactional(key, vendaRequest);
            });

            assertThat(exception.getMessage()).isEqualTo("Erro ao enviar venda via transação");
            assertThat(exception.getCause().getMessage()).isEqualTo("Transação falhou");
        }

        @Test
        @DisplayName("Deve tratar erro de serialização na transação")
        void deveTratarErroSerializacaoNaTransacao() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String key = "transactional-key";

            when(objectMapper.writeValueAsString(vendaRequest))
                .thenThrow(new RuntimeException("Erro de serialização"));

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                vendaProducer.sendTransactional(key, vendaRequest);
            });

            assertThat(exception.getMessage()).isEqualTo("Erro ao enviar venda via transação");
            assertThat(exception.getCause().getMessage()).isEqualTo("Erro de serialização");

            // Verifica que a transação não foi executada
            verify(kafkaTemplate, never()).executeInTransaction(any());
        }
    }

    @Nested
    @DisplayName("Testes de integração com diferentes cenários")
    class IntegracaoTests {

        @Test
        @DisplayName("Deve funcionar com chaves diferentes")
        void deveFuncionarComChavesDiferentes() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String[] keys = {"cliente-1", "cliente-2", "", null};
            String jsonPayload = "{}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);

            // Mock mais robusto que sempre retorna um Future válido para qualquer combinação de argumentos
            lenient().when(kafkaTemplate.send(any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(mock(SendResult.class)));

            // Captores para verificar argumentos
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);

            // Act
            for (String key : keys) {
                assertDoesNotThrow(() -> vendaProducer.send(key, vendaRequest));
            }

            // Assert - Captura todas as invocações
            verify(kafkaTemplate, times(4)).send(topicCaptor.capture(), keyCaptor.capture(), payloadCaptor.capture());

            // Verifica que todas as chamadas foram feitas
            assertThat(topicCaptor.getAllValues())
                .allMatch(topic -> topicName.equals(topic))
                .hasSize(4);

            assertThat(keyCaptor.getAllValues())
                .containsExactly("cliente-1", "cliente-2", "", null);

            assertThat(payloadCaptor.getAllValues())
                .allMatch(payload -> jsonPayload.equals(payload))
                .hasSize(4);
        }

        @Test
        @DisplayName("Deve funcionar com diferentes objetos VendaRequest")
        void deveFuncionarComDiferentesObjetosVendaRequest() throws Exception {
            // Arrange
            VendaRequest[] vendas = {
                criarVendaValida(),
                criarVendaComValorAlto(),
                criarVendaComQuantidadeAlta()
            };

            when(objectMapper.writeValueAsString(any(VendaRequest.class))).thenReturn("{}");

            // Mock robusto que sempre retorna um Future válido para qualquer combinação de argumentos
            lenient().when(kafkaTemplate.send(any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(mock(SendResult.class)));

            // Act & Assert
            for (int i = 0; i < vendas.length; i++) {
                String key = "key-" + i;
                int finalI = i;
                assertDoesNotThrow(() -> vendaProducer.send(key, vendas[finalI]));
            }

            verify(kafkaTemplate, times(vendas.length)).send(eq(topicName), anyString(), anyString());
        }

        @Test
        @DisplayName("Deve enviar para Kafka com chave null")
        void deveEnviarParaKafkaComChaveNull() throws Exception {
            // Arrange
            VendaRequest vendaRequest = criarVendaValida();
            String jsonPayload = "{}";

            when(objectMapper.writeValueAsString(vendaRequest)).thenReturn(jsonPayload);

            // Mock específico para chave null
            lenient().when(kafkaTemplate.send(any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(mock(SendResult.class)));

            // Act
            assertDoesNotThrow(() -> vendaProducer.send(null, vendaRequest));

            // Assert - Verifica que foi chamado com null
            verify(kafkaTemplate).send(topicName, null, jsonPayload);
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
        venda.setQuantidade(1);
        venda.setEmailCliente("cliente@email.com");
        return venda;
    }

    private VendaRequest criarVendaComValorAlto() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(99999L);
        venda.setProduto("Produto Premium");
        venda.setValor(new BigDecimal("99999.99"));
        venda.setQuantidade(1);
        venda.setEmailCliente("vip@email.com");
        return venda;
    }

    private VendaRequest criarVendaComQuantidadeAlta() {
        VendaRequest venda = new VendaRequest();
        venda.setPedidoId(55555L);
        venda.setProduto("Produto em Massa");
        venda.setValor(new BigDecimal("10.00"));
        venda.setQuantidade(1000);
        venda.setEmailCliente("atacado@email.com");
        return venda;
    }
}
