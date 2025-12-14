package com.example.kafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configuração do Swagger/OpenAPI para documentação da API.
 *
 * @author Sistema Kafka
 * @version 1.0
 */
@Configuration
public class SwaggerConfig {

    @Value("${server.port:8080}")
    private String serverPort;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .servers(List.of(
                    new Server()
                        .url("http://localhost:" + serverPort)
                        .description("Servidor Local de Desenvolvimento")
                ))
                .info(new Info()
                    .title("API de Vendas com Kafka")
                    .description("""
                        API REST para processamento de vendas utilizando Apache Kafka como sistema de mensageria.
                        
                        ## 🚀 Características Principais:
                        
                        ### Confiabilidade
                        - **Exactly-once delivery**: Cada venda é processada exatamente uma vez
                        - **Idempotência**: Proteção contra duplicatas usando IDs únicos de pedido
                        - **Dead Letter Queue**: Mensagens com erro são enviadas para DLQ após tentativas
                        
                        ### Arquitetura
                        - **Producer**: Envia mensagens com garantias de entrega
                        - **Consumer**: Processa mensagens com acknowledgment manual
                        - **Kafka**: Sistema de mensageria distribuída
                        
                        ### Monitoramento
                        - **Kafdrop**: Interface web para visualizar tópicos (http://localhost:9000)
                        - **Actuator**: Endpoints de saúde e métricas
                        - **Logging**: Rastreamento detalhado de operações
                        
                        ## 📊 Como Usar:
                        
                        1. **Envie uma venda** via POST `/api/v1/vendas`
                        2. **Monitore o processamento** via logs ou Kafdrop
                        3. **Verifique a saúde** via GET `/api/v1/vendas/health`
                        
                        ## 🔧 Infraestrutura:
                        
                        Para executar localmente:
                        ```bash
                        docker-compose up -d  # Inicia Kafka e Zookeeper
                        mvn spring-boot:run   # Inicia a aplicação
                        ```
                        """)
                    .version("1.0.0")
                    .contact(new Contact()
                        .name("Equipe de Desenvolvimento")
                        .email("dev@empresa.com")
                        .url("https://github.com/empresa/kafka-vendas"))
                    .license(new License()
                        .name("MIT License")
                        .url("https://opensource.org/licenses/MIT"))
                );
    }
}
