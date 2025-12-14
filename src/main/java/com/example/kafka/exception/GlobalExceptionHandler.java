package com.example.kafka.exception;

import com.example.kafka.dto.VendaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * Tratador global de exceções para a API.
 *
 * Centraliza o tratamento de erros e padroniza as respostas de erro.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Trata erros de validação de campos.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationExceptions(
            MethodArgumentNotValidException ex) {

        Map<String, Object> response = new HashMap<>();
        Map<String, String> errors = new HashMap<>();

        ex.getBindingResult().getAllErrors().forEach((error) -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            errors.put(fieldName, errorMessage);
        });

        response.put("status", "VALIDATION_ERROR");
        response.put("message", "Dados inválidos fornecidos");
        response.put("errors", errors);

        log.warn("Erro de validação: {}", errors);

        return ResponseEntity.badRequest().body(response);
    }

    /**
     * Trata argumentos ilegais (regras de negócio).
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<VendaResponse> handleIllegalArgumentException(
            IllegalArgumentException ex) {

        VendaResponse response = new VendaResponse(
            "Erro de validação: " + ex.getMessage(),
            null,
            "VALIDATION_ERROR"
        );

        log.warn("Argumento ilegal: {}", ex.getMessage());

        return ResponseEntity.badRequest().body(response);
    }

    /**
     * Trata exceções genéricas do sistema.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<VendaResponse> handleGenericException(Exception ex) {

        VendaResponse response = new VendaResponse(
            "Erro interno do servidor. Tente novamente mais tarde.",
            null,
            "INTERNAL_ERROR"
        );

        log.error("Erro não tratado", ex);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    /**
     * Trata exceções de runtime específicas da aplicação.
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<VendaResponse> handleRuntimeException(RuntimeException ex) {

        VendaResponse response = new VendaResponse(
            "Erro no processamento: " + ex.getMessage(),
            null,
            "PROCESSING_ERROR"
        );

        log.error("Erro de runtime: {}", ex.getMessage(), ex);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
