package com.example.kafka.exception;

import com.example.kafka.dto.VendaResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Testes unitários para GlobalExceptionHandler.
 *
 * Exemplifica:
 * - Testes de tratamento de exceções específicas
 * - Verificação de status HTTP corretos
 * - Validação de mensagens de erro padronizadas
 * - Testes de diferentes tipos de exceção
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Testes para GlobalExceptionHandler")
class GlobalExceptionHandlerTest {

    @InjectMocks
    private GlobalExceptionHandler globalExceptionHandler;

    @Nested
    @DisplayName("Testes de tratamento de JSON malformado")
    class JsonMalformadoTests {

        @Test
        @DisplayName("Deve tratar HttpMessageNotReadableException com status 400")
        void deveTratarHttpMessageNotReadableExceptionComStatus400() {
            // Arrange
            HttpMessageNotReadableException ex = new HttpMessageNotReadableException(
                "JSON parse error: Unexpected character");

            // Act
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleHttpMessageNotReadableException(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getMessage())
                .isEqualTo("JSON malformado ou inválido. Verifique a estrutura da requisição.");
            assertThat(response.getBody().getStatus()).isEqualTo("INVALID_JSON");
            assertThat(response.getBody().getPedidoId()).isNull();
        }

        @Test
        @DisplayName("Deve tratar diferentes mensagens de erro JSON")
        void deveTratarDiferentesMensagensDeErroJson() {
            // Arrange - Diferentes tipos de erro JSON
            String[] mensagensErro = {
                "Unexpected character ('i' (code 105)): was expecting double-quote to start field name",
                "Unexpected end-of-input: expected close marker for Object",
                "Unrecognized token 'invalid': was expecting ('true', 'false' or 'null')",
                "Invalid numeric value: Leading zeroes not allowed"
            };

            for (String mensagem : mensagensErro) {
                // Arrange
                HttpMessageNotReadableException ex = new HttpMessageNotReadableException(mensagem);

                // Act
                ResponseEntity<VendaResponse> response = globalExceptionHandler
                    .handleHttpMessageNotReadableException(ex);

                // Assert - Todos devem retornar a mesma resposta padronizada
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                assertThat(response.getBody().getStatus()).isEqualTo("INVALID_JSON");
                assertThat(response.getBody().getMessage())
                    .isEqualTo("JSON malformado ou inválido. Verifique a estrutura da requisição.");
            }
        }
    }

    @Nested
    @DisplayName("Testes de tratamento de validação")
    class ValidacaoTests {

        @Test
        @DisplayName("Deve tratar MethodArgumentNotValidException com status 400")
        void deveTratarMethodArgumentNotValidExceptionComStatus400() {
            // Arrange
            MethodArgumentNotValidException ex = mock(MethodArgumentNotValidException.class);
            BindingResult bindingResult = mock(BindingResult.class);

            FieldError fieldError1 = new FieldError("vendaRequest", "pedidoId", "ID do pedido é obrigatório");
            FieldError fieldError2 = new FieldError("vendaRequest", "produto", "Nome do produto é obrigatório");

            when(ex.getBindingResult()).thenReturn(bindingResult);
            when(bindingResult.getAllErrors()).thenReturn(Arrays.asList(fieldError1, fieldError2));

            // Act
            ResponseEntity<Map<String, Object>> response = globalExceptionHandler
                .handleValidationExceptions(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().get("status")).isEqualTo("VALIDATION_ERROR");
            assertThat(response.getBody().get("message")).isEqualTo("Dados inválidos fornecidos");

            @SuppressWarnings("unchecked")
            Map<String, String> errors = (Map<String, String>) response.getBody().get("errors");
            assertThat(errors).hasSize(2);
            assertThat(errors.get("pedidoId")).isEqualTo("ID do pedido é obrigatório");
            assertThat(errors.get("produto")).isEqualTo("Nome do produto é obrigatório");
        }

        @Test
        @DisplayName("Deve tratar IllegalArgumentException com status 400")
        void deveTratarIllegalArgumentExceptionComStatus400() {
            // Arrange
            IllegalArgumentException ex = new IllegalArgumentException("Valor deve ser positivo");

            // Act
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleIllegalArgumentException(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getMessage()).isEqualTo("Erro de validação: Valor deve ser positivo");
            assertThat(response.getBody().getStatus()).isEqualTo("VALIDATION_ERROR");
            assertThat(response.getBody().getPedidoId()).isNull();
        }
    }

    @Nested
    @DisplayName("Testes de tratamento de erros internos")
    class ErrosInternosTests {

        @Test
        @DisplayName("Deve tratar RuntimeException com status 500")
        void deveTratarRuntimeExceptionComStatus500() {
            // Arrange
            RuntimeException ex = new RuntimeException("Kafka indisponível");

            // Act
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleRuntimeException(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getMessage()).isEqualTo("Erro no processamento: Kafka indisponível");
            assertThat(response.getBody().getStatus()).isEqualTo("PROCESSING_ERROR");
            assertThat(response.getBody().getPedidoId()).isNull();
        }

        @Test
        @DisplayName("Deve tratar Exception genérica com status 500")
        void deveTratarExceptionGenericaComStatus500() {
            // Arrange
            Exception ex = new Exception("Erro inesperado");

            // Act
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleGenericException(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getMessage())
                .isEqualTo("Erro interno do servidor. Tente novamente mais tarde.");
            assertThat(response.getBody().getStatus()).isEqualTo("INTERNAL_ERROR");
            assertThat(response.getBody().getPedidoId()).isNull();
        }
    }

    @Nested
    @DisplayName("Testes de precedência de handlers")
    class PrecedenciaHandlersTests {

        @Test
        @DisplayName("HttpMessageNotReadableException deve ter precedência sobre RuntimeException")
        void httpMessageNotReadableExceptionDeveTerPrecedenciaSobreRuntimeException() {
            // Arrange - HttpMessageNotReadableException é uma subclasse de RuntimeException
            HttpMessageNotReadableException ex = new HttpMessageNotReadableException("JSON inválido");

            // Act - Deve chamar o handler específico, não o genérico
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleHttpMessageNotReadableException(ex);

            // Assert - Deve retornar 400, não 500
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody().getStatus()).isEqualTo("INVALID_JSON");
            // Não deve ser "PROCESSING_ERROR" que seria do RuntimeException handler
        }

        @Test
        @DisplayName("IllegalArgumentException deve ter precedência sobre RuntimeException")
        void illegalArgumentExceptionDeveTerPrecedenciaSobreRuntimeException() {
            // Arrange - IllegalArgumentException é uma subclasse de RuntimeException
            IllegalArgumentException ex = new IllegalArgumentException("Argumento inválido");

            // Act - Deve chamar o handler específico, não o genérico
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleIllegalArgumentException(ex);

            // Assert - Deve retornar 400, não 500
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody().getStatus()).isEqualTo("VALIDATION_ERROR");
            // Não deve ser "PROCESSING_ERROR" que seria do RuntimeException handler
        }
    }

    @Nested
    @DisplayName("Testes de tratamento de tipo de mídia não suportado")
    class TipoMidiaNaoSuportadoTests {

        @Test
        @DisplayName("Deve tratar HttpMediaTypeNotSupportedException com status 415")
        void deveTratarHttpMediaTypeNotSupportedExceptionComStatus415() {
            // Arrange
            HttpMediaTypeNotSupportedException ex = new HttpMediaTypeNotSupportedException(
                "Content-Type 'text/plain' is not supported");

            // Act
            ResponseEntity<VendaResponse> response = globalExceptionHandler
                .handleHttpMediaTypeNotSupportedException(ex);

            // Assert
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getMessage())
                .isEqualTo("Content-Type não suportado. Use 'application/json' para esta API.");
            assertThat(response.getBody().getStatus()).isEqualTo("UNSUPPORTED_MEDIA_TYPE");
            assertThat(response.getBody().getPedidoId()).isNull();
        }
    }
}
