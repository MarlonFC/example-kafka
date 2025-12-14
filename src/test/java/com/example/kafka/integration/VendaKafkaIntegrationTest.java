package com.example.kafka.integration;

import com.example.kafka.dto.VendaRequest;
import com.example.kafka.dto.VendaResponse;
import com.example.kafka.service.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

/**
 * Testes de integração completos para o sistema de vendas com Kafka.
 *
 * Exemplifica:
 * - Testes de ponta a ponta (controller → producer → consumer → service)
 * - Uso de EmbeddedKafka para testes de integração
 * - Testes de idempotência real
 * - Testes de processamento assíncrono
 * - Verificação de estado final do sistema
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    partitions = 3,
    topics = {"vendas"},
    controlledShutdown = true,
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:0",
        "port=0"
    }
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
    "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
    "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
    "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
    "spring.kafka.consumer.group-id=test-group",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "app.kafka.topic.vendas=vendas"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("Testes de Integração - Sistema de Vendas Kafka")
class VendaKafkaIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private IdempotencyService idempotencyService;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port + "/api/v1/vendas";
        idempotencyService.clear(); // Limpa cache entre testes
    }

    @Test
    @DisplayName("Deve processar venda completa de ponta a ponta")
    void deveProcessarVendaCompletaDePontaAPonta() {
        // Arrange
        VendaRequest vendaRequest = criarVendaValida();

        // Act - Envia venda via REST API
        ResponseEntity<VendaResponse> response = restTemplate.postForEntity(
            baseUrl, vendaRequest, VendaResponse.class
        );

        // Assert - Resposta HTTP
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("ACCEPTED");
        assertThat(response.getBody().getPedidoId()).isEqualTo(vendaRequest.getPedidoId());

        // Assert - Processamento assíncrono (aguarda até 10 segundos)
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(idempotencyService.isAlreadyProcessed(vendaRequest.getPedidoId().toString()))
                    .isTrue();
            });
    }

    @Test
    @DisplayName("Deve garantir idempotência em processamento duplicado")
    void deveGarantirIdempotenciaEmProcessamentoDuplicado() {
        // Arrange
        VendaRequest vendaRequest = criarVendaValida();

        // Act - Envia a mesma venda múltiplas vezes
        ResponseEntity<VendaResponse> response1 = restTemplate.postForEntity(
            baseUrl, vendaRequest, VendaResponse.class
        );
        ResponseEntity<VendaResponse> response2 = restTemplate.postForEntity(
            baseUrl, vendaRequest, VendaResponse.class
        );
        ResponseEntity<VendaResponse> response3 = restTemplate.postForEntity(
            baseUrl, vendaRequest, VendaResponse.class
        );

        // Assert - Todas as respostas devem ser ACCEPTED
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response3.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Assert - Aguarda processamento e verifica idempotência
        await()
            .atMost(15, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                // Deve ter processado apenas uma vez
                assertThat(idempotencyService.getProcessedCount()).isEqualTo(1);
                assertThat(idempotencyService.isAlreadyProcessed(vendaRequest.getPedidoId().toString()))
                    .isTrue();
            });
    }

    @Test
    @DisplayName("Deve processar múltiplas vendas distintas")
    void deveProcessarMultiplasVendasDistintas() {
        // Arrange
        VendaRequest venda1 = criarVendaValida();
        venda1.setPedidoId(10001L);

        VendaRequest venda2 = criarVendaValida();
        venda2.setPedidoId(10002L);
        venda2.setProduto("Smartphone Samsung");

        VendaRequest venda3 = criarVendaValida();
        venda3.setPedidoId(10003L);
        venda3.setProduto("Tablet Apple");

        // Act - Envia todas as vendas
        ResponseEntity<VendaResponse> response1 = restTemplate.postForEntity(baseUrl, venda1, VendaResponse.class);
        ResponseEntity<VendaResponse> response2 = restTemplate.postForEntity(baseUrl, venda2, VendaResponse.class);
        ResponseEntity<VendaResponse> response3 = restTemplate.postForEntity(baseUrl, venda3, VendaResponse.class);

        // Assert - Todas devem ser aceitas
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response3.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Assert - Todas devem ser processadas
        await()
            .atMost(15, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(idempotencyService.getProcessedCount()).isEqualTo(3);
                assertThat(idempotencyService.isAlreadyProcessed("10001")).isTrue();
                assertThat(idempotencyService.isAlreadyProcessed("10002")).isTrue();
                assertThat(idempotencyService.isAlreadyProcessed("10003")).isTrue();
            });
    }

    @Test
    @DisplayName("Deve processar venda com chave customizada")
    void deveProcessarVendaComChaveCustomizada() {
        // Arrange
        VendaRequest vendaRequest = criarVendaValida();
        String chaveCustomizada = "cliente-vip-123";

        // Act
        ResponseEntity<VendaResponse> response = restTemplate.postForEntity(
            baseUrl + "?key=" + chaveCustomizada, vendaRequest, VendaResponse.class
        );

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Aguarda processamento
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(idempotencyService.isAlreadyProcessed(vendaRequest.getPedidoId().toString()))
                    .isTrue();
            });
    }

    @Test
    @DisplayName("Deve rejeitar venda com dados inválidos")
    void deveRejeitarVendaComDadosInvalidos() {
        // Arrange
        VendaRequest vendaInvalida = new VendaRequest();
        vendaInvalida.setPedidoId(null); // Campo obrigatório
        vendaInvalida.setProduto(""); // Campo em branco
        vendaInvalida.setValor(new BigDecimal("-100")); // Valor negativo
        vendaInvalida.setQuantidade(0); // Quantidade zero
        vendaInvalida.setEmailCliente(""); // Email em branco

        // Act
        ResponseEntity<String> response = restTemplate.postForEntity(
            baseUrl, vendaInvalida, String.class
        );

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        // Verifica que nada foi processado
        await()
            .pollDelay(Duration.ofSeconds(2))
            .atMost(5, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                assertThat(idempotencyService.getProcessedCount()).isEqualTo(0);
            });
    }

    @Test
    @DisplayName("Deve funcionar health check")
    void deveFuncionarHealthCheck() {
        // Act
        ResponseEntity<String> response = restTemplate.getForEntity(
            baseUrl + "/health", String.class
        );

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("Serviço de vendas funcionando!");
    }

    @Test
    @DisplayName("Deve processar vendas com alta concorrência")
    void deveProcessarVendasComAltaConcorrencia() throws InterruptedException {
        // Arrange
        int numeroVendas = 50;
        Thread[] threads = new Thread[numeroVendas];

        // Act - Simula múltiplas requisições simultâneas
        for (int i = 0; i < numeroVendas; i++) {
            final long pedidoId = 20000L + i;
            threads[i] = new Thread(() -> {
                VendaRequest venda = criarVendaValida();
                venda.setPedidoId(pedidoId);
                venda.setProduto("Produto Concorrente " + pedidoId);

                ResponseEntity<VendaResponse> response = restTemplate.postForEntity(
                    baseUrl, venda, VendaResponse.class
                );

                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            });
        }

        // Inicia todas as threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Aguarda todas terminarem
        for (Thread thread : threads) {
            thread.join();
        }

        // Assert - Todas as vendas devem ser processadas
        await()
            .atMost(30, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(idempotencyService.getProcessedCount()).isEqualTo(numeroVendas);
            });
    }

    @Test
    @DisplayName("Deve processar vendas com caracteres especiais")
    void deveProcessarVendasComCaracteresEspeciais() {
        // Arrange
        VendaRequest vendaEspecial = new VendaRequest();
        vendaEspecial.setPedidoId(30001L);
        vendaEspecial.setProduto("Produto com àçêñtôs & símb@los #especiais! 中文 🎉");
        vendaEspecial.setValor(new BigDecimal("1500.50"));
        vendaEspecial.setQuantidade(2);
        vendaEspecial.setEmailCliente("usuário.téste+tag@exêmplo-empresa.com.br");

        // Act
        ResponseEntity<VendaResponse> response = restTemplate.postForEntity(
            baseUrl, vendaEspecial, VendaResponse.class
        );

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollDelay(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(idempotencyService.isAlreadyProcessed("30001")).isTrue();
            });
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
