package com.example.kafka.controller;

import com.example.kafka.service.IdempotencyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Controller para monitoramento e debug do sistema Kafka.
 *
 * Fornece endpoints para visualizar informações sobre offsets, consumer groups
 * e estado do sistema de mensageria.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Tag(name = "Monitoramento", description = "Endpoints para monitoramento do sistema Kafka")
@RestController
@RequestMapping("/api/v1/monitoring")
public class MonitoringController {

    private static final Logger log = LoggerFactory.getLogger(MonitoringController.class);

    private final IdempotencyService idempotencyService;
    private final KafkaAdmin kafkaAdmin;
    private final KafkaListenerEndpointRegistry endpointRegistry;

    public MonitoringController(IdempotencyService idempotencyService,
                               KafkaAdmin kafkaAdmin,
                               KafkaListenerEndpointRegistry endpointRegistry) {
        this.idempotencyService = idempotencyService;
        this.kafkaAdmin = kafkaAdmin;
        this.endpointRegistry = endpointRegistry;
    }

    @Operation(
        summary = "Informações sobre __consumer_offsets",
        description = "Mostra informações detalhadas sobre como funciona o tópico __consumer_offsets"
    )
    @ApiResponse(responseCode = "200", description = "Informações sobre offsets")
    @GetMapping("/consumer-offsets-info")
    public Map<String, Object> getConsumerOffsetsInfo() {
        Map<String, Object> info = new HashMap<>();

        info.put("explanation", "O __consumer_offsets é um tópico interno do Kafka que armazena o progresso de cada consumer group");
        info.put("partition_formula", "partição = hash(group_id) % 50");
        info.put("our_group_id", "vendas-consumer-group");
        info.put("estimated_partition", Math.abs("vendas-consumer-group".hashCode() % 50));

        Map<String, Object> messageStructure = new HashMap<>();
        messageStructure.put("key_format", "{group: 'vendas-consumer-group', topic: 'vendas', partition: 0}");
        messageStructure.put("value_format", "{offset: X, metadata: '', commit_timestamp: timestamp}");

        info.put("message_structure", messageStructure);
        info.put("cleanup_policy", "compact - mantém apenas o offset mais recente para cada chave");
        info.put("how_to_view", "Acesse Kafdrop em http://localhost:9000 e clique no tópico __consumer_offsets");

        return info;
    }

    @Operation(
        summary = "Status do consumer group",
        description = "Mostra informações sobre o nosso consumer group e offsets"
    )
    @GetMapping("/consumer-group-status")
    public Map<String, Object> getConsumerGroupStatus() {
        Map<String, Object> status = new HashMap<>();

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Lista consumer groups
            ListConsumerGroupsResult groups = adminClient.listConsumerGroups();

            status.put("total_consumer_groups", groups.all().get().size());
            status.put("our_group_id", "vendas-consumer-group");

            // Verifica se nosso group existe
            boolean ourGroupExists = groups.all().get().stream()
                .anyMatch(group -> "vendas-consumer-group".equals(group.groupId()));

            status.put("our_group_active", ourGroupExists);

            if (ourGroupExists) {
                ConsumerGroupDescription description = adminClient
                    .describeConsumerGroups(java.util.Set.of("vendas-consumer-group"))
                    .describedGroups()
                    .get("vendas-consumer-group")
                    .get();

                status.put("group_state", description.state().toString());
                status.put("members_count", description.members().size());
            }

        } catch (Exception e) {
            log.warn("Erro ao consultar status do consumer group", e);
            status.put("error", "Não foi possível consultar status: " + e.getMessage());
        }

        return status;
    }

    @Operation(
        summary = "Estatísticas de processamento",
        description = "Mostra estatísticas sobre vendas processadas (cache de idempotência)"
    )
    @GetMapping("/processing-stats")
    public Map<String, Object> getProcessingStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("processed_orders_count", idempotencyService.getProcessedCount());
        stats.put("note", "Este número representa quantas vendas únicas foram processadas");
        stats.put("idempotency_explanation",
            "Cada venda com pedidoId único incrementa este contador. " +
            "Vendas duplicadas são rejeitadas e não incrementam o contador.");

        return stats;
    }

    @Operation(
        summary = "Simular offset commits",
        description = "Explica como os offsets são commitados quando uma venda é processada"
    )
    @GetMapping("/offset-commit-simulation")
    public Map<String, Object> simulateOffsetCommit() {
        Map<String, Object> simulation = new HashMap<>();

        simulation.put("step_1", "Cliente envia POST /api/v1/vendas");
        simulation.put("step_2", "VendaProducer envia mensagem para tópico 'vendas'");
        simulation.put("step_3", "VendaConsumer recebe mensagem");
        simulation.put("step_4", "IdempotencyService verifica duplicata");
        simulation.put("step_5", "VendaProcessingService processa a venda");
        simulation.put("step_6", "ack.acknowledge() é chamado");
        simulation.put("step_7", "Kafka cria entrada em __consumer_offsets");

        Map<String, String> offsetEntry = new HashMap<>();
        offsetEntry.put("topic", "__consumer_offsets");
        offsetEntry.put("partition", "hash('vendas-consumer-group') % 50");
        offsetEntry.put("key", "{group:'vendas-consumer-group',topic:'vendas',partition:0}");
        offsetEntry.put("value", "{offset:X,commit_timestamp:now}");

        simulation.put("offset_entry_created", offsetEntry);
        simulation.put("visible_in_kafdrop", "Sim - você pode ver esta entrada no Kafdrop");

        return simulation;
    }

    @Operation(
        summary = "Limpar cache de idempotência",
        description = "Limpa o cache em memória de vendas processadas (apenas para testes)"
    )
    @DeleteMapping("/clear-processed-cache")
    public Map<String, Object> clearProcessedCache() {
        int previousCount = idempotencyService.getProcessedCount();
        idempotencyService.clear();

        Map<String, Object> result = new HashMap<>();
        result.put("previous_count", previousCount);
        result.put("current_count", idempotencyService.getProcessedCount());
        result.put("message", "Cache de idempotência limpo com sucesso");
        result.put("note", "Isso NÃO afeta os offsets no __consumer_offsets - apenas o cache em memória");

        log.info("Cache de idempotência limpo via API - {} entradas removidas", previousCount);

        return result;
    }

    @Operation(
        summary = "Guia de debugging",
        description = "Fornece um guia sobre como debuggar problemas com offsets"
    )
    @GetMapping("/debugging-guide")
    public Map<String, Object> getDebuggingGuide() {
        Map<String, Object> guide = new HashMap<>();

        guide.put("kafdrop_access", "http://localhost:9000");
        guide.put("steps_to_debug", java.util.List.of(
            "1. Acesse Kafdrop em http://localhost:9000",
            "2. Clique no tópico '__consumer_offsets'",
            "3. Procure pela partição do seu consumer group",
            "4. Examine as mensagens de commit de offset",
            "5. Compare com os logs da aplicação"
        ));

        guide.put("common_issues", java.util.Map.of(
            "consumer_lag", "Diferença entre último offset disponível e último processado",
            "duplicate_processing", "Falha no commit de offset - verificar logs de erro",
            "offset_reset", "Consumer começou do início - verificar configuração auto-offset-reset"
        ));

        guide.put("useful_kafka_commands", java.util.List.of(
            "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vendas-consumer-group",
            "kafka-topics.sh --bootstrap-server localhost:9092 --list",
            "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic __consumer_offsets --from-beginning"
        ));

        return guide;
    }
}
