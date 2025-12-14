package com.example.kafka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para IdempotencyService.
 *
 * Exemplifica:
 * - Testes de idempotência e operações thread-safe
 * - Testes de métodos de controle de estado
 * - Validação de comportamento atômico
 * - Testes com valores null e vazios
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Testes para IdempotencyService")
class IdempotencyServiceTest {

    private IdempotencyService idempotencyService;

    @BeforeEach
    void setUp() {
        idempotencyService = new IdempotencyService();
    }

    @Nested
    @DisplayName("Testes de verificação de processamento")
    class VerificacaoProcessamentoTests {

        @Test
        @DisplayName("Deve retornar false para ID não processado")
        void deveRetornarFalseParaIdNaoProcessado() {
            // Arrange
            String orderId = "pedido-novo";

            // Act
            boolean jaProcessado = idempotencyService.isAlreadyProcessed(orderId);

            // Assert
            assertThat(jaProcessado).isFalse();
        }

        @Test
        @DisplayName("Deve retornar true para ID já processado")
        void deveRetornarTrueParaIdJaProcessado() {
            // Arrange
            String orderId = "pedido-processado";
            idempotencyService.markAsProcessed(orderId);

            // Act
            boolean jaProcessado = idempotencyService.isAlreadyProcessed(orderId);

            // Assert
            assertThat(jaProcessado).isTrue();
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("Deve retornar false para ID null ou vazio")
        void deveRetornarFalseParaIdNullOuVazio(String orderId) {
            // Act
            boolean jaProcessado = idempotencyService.isAlreadyProcessed(orderId);

            // Assert
            assertThat(jaProcessado).isFalse();
        }

        @ParameterizedTest
        @ValueSource(strings = {"  ", "\t", "\n", "\r\n"})
        @DisplayName("Deve retornar false para ID com apenas espaços em branco")
        void deveRetornarFalseParaIdComApenasEspacosEmBranco(String orderId) {
            // Act
            boolean jaProcessado = idempotencyService.isAlreadyProcessed(orderId);

            // Assert
            assertThat(jaProcessado).isFalse();
        }
    }

    @Nested
    @DisplayName("Testes de marcação como processado")
    class MarcacaoProcessadoTests {

        @Test
        @DisplayName("Deve marcar ID como processado")
        void deveMarcaIdComoProcessado() {
            // Arrange
            String orderId = "pedido-123";

            // Act
            idempotencyService.markAsProcessed(orderId);

            // Assert
            assertThat(idempotencyService.isAlreadyProcessed(orderId)).isTrue();
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Não deve aumentar contagem para ID duplicado")
        void naoDeveAumentarContagemParaIdDuplicado() {
            // Arrange
            String orderId = "pedido-duplicado";

            // Act - Marca duas vezes o mesmo ID
            idempotencyService.markAsProcessed(orderId);
            idempotencyService.markAsProcessed(orderId);

            // Assert - Contagem deve ser 1, não 2
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
            assertThat(idempotencyService.isAlreadyProcessed(orderId)).isTrue();
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("Deve lidar com ID null ou vazio sem erro")
        void deveLidarComIdNullOuVazioSemErro(String orderId) {
            // Act & Assert - Não deve lançar exceção
            assertDoesNotThrow(() -> idempotencyService.markAsProcessed(orderId));

            // Contagem deve permanecer zero
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("Testes de operação atômica canProcessAndMark")
    class CanProcessAndMarkTests {

        @Test
        @DisplayName("Deve permitir processamento de novo ID")
        void devePermitirProcessamentoDeNovoId() {
            // Arrange
            String orderId = "pedido-novo";

            // Act
            boolean canProcess = idempotencyService.canProcessAndMark(orderId);

            // Assert
            assertThat(canProcess).isTrue();
            assertThat(idempotencyService.isAlreadyProcessed(orderId)).isTrue();
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Deve negar processamento de ID já processado")
        void deveNegarProcessamentoDeIdJaProcessado() {
            // Arrange
            String orderId = "pedido-existente";
            idempotencyService.markAsProcessed(orderId);

            // Act
            boolean canProcess = idempotencyService.canProcessAndMark(orderId);

            // Assert
            assertThat(canProcess).isFalse();
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1); // Não aumenta
        }

        @Test
        @DisplayName("Deve permitir processamento para ID null ou vazio")
        void devePermitirProcessamentoParaIdNullOuVazio() {
            // Act & Assert - Por compatibilidade, permite null e vazio mas não incrementa contagem
            assertThat(idempotencyService.canProcessAndMark(null)).isTrue();
            assertThat(idempotencyService.canProcessAndMark("")).isTrue();
            assertThat(idempotencyService.canProcessAndMark("   ")).isTrue();

            // Contagem deve permanecer zero pois IDs inválidos não são armazenados
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("Deve ser thread-safe - múltiplas chamadas simultâneas")
        void deveSerThreadSafeMultiplasChaMadasSimultaneas() {
            // Arrange
            String orderId = "pedido-concurrent";
            int threadCount = 10;
            boolean[] results = new boolean[threadCount];

            // Act - Simula múltiplas threads tentando processar o mesmo ID
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                final int index = i;
                threads[i] = new Thread(() -> {
                    results[index] = idempotencyService.canProcessAndMark(orderId);
                });
            }

            // Inicia todas as threads
            for (Thread thread : threads) {
                thread.start();
            }

            // Aguarda todas terminarem
            for (Thread thread : threads) {
                assertDoesNotThrow(() -> thread.join());
            }

            // Assert - Apenas uma thread deve ter retornado true
            int trueCount = 0;
            for (boolean result : results) {
                if (result) {
                    trueCount++;
                }
            }

            assertThat(trueCount).isEqualTo(1);
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
            assertThat(idempotencyService.isAlreadyProcessed(orderId)).isTrue();
        }
    }

    @Nested
    @DisplayName("Testes de operações auxiliares")
    class OperacoesAuxiliaresTests {

        @Test
        @DisplayName("Deve limpar cache corretamente")
        void deveLimparCacheCorretamente() {
            // Arrange - Adiciona alguns IDs
            idempotencyService.markAsProcessed("pedido-1");
            idempotencyService.markAsProcessed("pedido-2");
            idempotencyService.markAsProcessed("pedido-3");
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(3);

            // Act
            idempotencyService.clear();

            // Assert
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);
            assertThat(idempotencyService.isAlreadyProcessed("pedido-1")).isFalse();
            assertThat(idempotencyService.isAlreadyProcessed("pedido-2")).isFalse();
            assertThat(idempotencyService.isAlreadyProcessed("pedido-3")).isFalse();
        }

        @Test
        @DisplayName("Deve remover ID específico do cache")
        void deveRemoverIdEspecificoDoCache() {
            // Arrange
            String orderIdParaRemover = "pedido-remover";
            String orderIdParaManter = "pedido-manter";

            idempotencyService.markAsProcessed(orderIdParaRemover);
            idempotencyService.markAsProcessed(orderIdParaManter);
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(2);

            // Act
            boolean removido = idempotencyService.removeProcessed(orderIdParaRemover);

            // Assert
            assertThat(removido).isTrue();
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
            assertThat(idempotencyService.isAlreadyProcessed(orderIdParaRemover)).isFalse();
            assertThat(idempotencyService.isAlreadyProcessed(orderIdParaManter)).isTrue();
        }

        @Test
        @DisplayName("Deve retornar false ao tentar remover ID inexistente")
        void deveRetornarFalseAoTentarRemoverIdInexistente() {
            // Act
            boolean removido = idempotencyService.removeProcessed("pedido-inexistente");

            // Assert
            assertThat(removido).isFalse();
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("Deve retornar false para remoção de ID null ou vazio")
        void deveRetornarFalseParaRemocaoDeIdNullOuVazio(String orderId) {
            // Act
            boolean removido = idempotencyService.removeProcessed(orderId);

            // Assert
            assertThat(removido).isFalse();
        }

        @Test
        @DisplayName("Deve contar IDs processados corretamente")
        void deveContarIdsProcessadosCorretamente() {
            // Arrange & Act
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);

            idempotencyService.markAsProcessed("pedido-1");
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);

            idempotencyService.markAsProcessed("pedido-2");
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(2);

            // Adiciona o mesmo ID novamente - não deve aumentar
            idempotencyService.markAsProcessed("pedido-1");
            assertThat(idempotencyService.getProcessedCount()).isEqualTo(2);
        }
    }
}
