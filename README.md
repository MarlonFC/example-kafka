# 🛍️ API de Vendas com Apache Kafka

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.java.net/projects/jdk/17/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.1-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8+-red.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

Sistema de processamento de vendas utilizando **Apache Kafka** como sistema de mensageria, garantindo **exactly-once delivery** e alta confiabilidade.

## 🏗️ Arquitetura

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Cliente   │───▶│ REST API    │───▶│    Kafka    │───▶│  Consumer   │
│             │    │ Controller  │    │   Topic     │    │ Processor   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                          │                                      │
                          ▼                                      ▼
                   ┌─────────────┐                        ┌─────────────┐
                   │  Producer   │                        │ Idempotency │
                   │   Service   │                        │   Service   │
                   └─────────────┘                        └─────────────┘
```

### 🎯 Características Principais

- ✅ **Exactly-once delivery**: Cada venda é processada exatamente uma vez
- ✅ **Idempotência**: Proteção contra duplicatas usando IDs únicos
- ✅ **Dead Letter Queue**: Mensagens com erro são enviadas para DLQ
- ✅ **Acknowledgment manual**: Controle preciso sobre processamento
- ✅ **Documentação Swagger**: API documentada e testável
- ✅ **Monitoramento**: Métricas e health checks
- ✅ **Tratamento de erros**: Retry automático e recuperação

## 🚀 Quick Start

### Pré-requisitos

- **Java 17+**
- **Maven 3.8+**
- **Docker & Docker Compose**

### 1. Clone o repositório

```bash
git clone <repository-url>
cd example-kafka
```

### 2. Inicie a infraestrutura

```bash
# Inicia Kafka, Zookeeper e Kafdrop
docker-compose up -d

# Aguarde uns 30 segundos para inicialização completa
docker-compose logs kafka
```

### 3. Execute a aplicação

```bash
# Via Maven
mvn spring-boot:run

# Ou via JAR
mvn clean package
java -jar target/kafka-exercices-0.0.1-SNAPSHOT.jar
```

### 4. Teste a API

```bash
# Envie uma venda
curl -X POST http://localhost:8080/api/v1/vendas \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": 12345,
    "produto": "Notebook Dell Inspiron",
    "valor": 2500.99,
    "quantidade": 1,
    "emailCliente": "cliente@email.com"
  }'

# Resposta esperada:
{
  "message": "Venda enviada para processamento",
  "pedidoId": 12345,
  "status": "ACCEPTED"
}
```

## 📚 Documentação da API

### Swagger UI
Acesse: **http://localhost:8080/swagger-ui.html**

### Endpoints Principais

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/api/v1/vendas` | Cria nova venda |
| `GET` | `/api/v1/vendas/health` | Health check |
| `GET` | `/actuator/health` | Status da aplicação |
| `GET` | `/actuator/metrics` | Métricas |

### Exemplo de Request

```json
{
  "pedidoId": 12345,
  "produto": "Smartphone Samsung Galaxy",
  "valor": 1299.99,
  "quantidade": 2,
  "emailCliente": "joao@email.com"
}
```

### Exemplo de Response (202 Accepted)

```json
{
  "message": "Venda enviada para processamento",
  "pedidoId": 12345,
  "status": "ACCEPTED"
}
```

## 🔧 Configurações

### application.yaml

```yaml
# Kafka Configuration
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: vendas-consumer-group
      auto-offset-reset: earliest

# Custom Settings
app:
  kafka:
    topic:
      vendas: vendas
    retries:
      max: 3
      backoff-ms: 1000
```

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Endereço do Kafka |
| `KAFKA_GROUP_ID` | `vendas-consumer-group` | ID do grupo consumidor |
| `SERVER_PORT` | `8080` | Porta da aplicação |

## 🐳 Docker

### docker-compose.yml

O projeto inclui configuração completa com:

- **Zookeeper**: Coordenação do cluster Kafka
- **Kafka**: Broker de mensagens (porta 9092)
- **Kafdrop**: Interface web para monitoramento (porta 9000)

```bash
# Inicia todos os serviços
docker-compose up -d

# Para todos os serviços
docker-compose down

# Visualiza logs
docker-compose logs -f kafka
```

## 📊 Monitoramento

### Kafdrop UI
- **URL**: http://localhost:9000
- **Funcionalidades**: 
  - Visualizar tópicos e partições
  - Monitorar offset dos consumidores
  - Examinar mensagens
  - Verificar dead letter queues

### Actuator Endpoints
- **Health**: http://localhost:8080/actuator/health
- **Metrics**: http://localhost:8080/actuator/metrics
- **Info**: http://localhost:8080/actuator/info

## 🧪 Testando Idempotência

```bash
# Envie a mesma venda 3 vezes
for i in {1..3}; do
  curl -X POST http://localhost:8080/api/v1/vendas \
    -H "Content-Type: application/json" \
    -d '{
      "pedidoId": 99999,
      "produto": "Teste Idempotência",
      "valor": 100.00,
      "quantidade": 1,
      "emailCliente": "teste@email.com"
    }'
  echo "Envio $i concluído"
done

# Verifique os logs: apenas 1 processamento deve ocorrer!
```

## 🔍 Logs

Os logs são configurados para fornecer visibilidade completa:

```bash
# Via Docker
docker-compose logs -f app

# Via arquivo (quando configurado)
tail -f logs/vendas-kafka.log
```

### Exemplo de Log de Processamento

```
2024-12-14 10:30:15 [kafka-consumer-1] INFO  [VendaConsumer] - Recebida mensagem - key=12345, offset=0, partition=0
2024-12-14 10:30:15 [kafka-consumer-1] INFO  [IdempotencyService] - OrderId 12345 é NOVO - marcado como processado. Total agora: 1
2024-12-14 10:30:15 [kafka-consumer-1] INFO  [VendaProcessingService] - Iniciando processamento da venda - orderId=12345, pedidoId=12345
2024-12-14 10:30:15 [kafka-consumer-1] INFO  [VendaConsumer] - Venda processada com sucesso - orderId=12345, pedidoId=12345
```

## 🛡️ Garantias de Confiabilidade

### Producer (Exatamente Uma Vez)
- `enable.idempotence=true`
- `acks=all`
- `retries=Integer.MAX_VALUE`
- `max.in.flight.requests.per.connection=1`

### Consumer (Processamento Seguro)
- `enable.auto.commit=false` (acknowledgment manual)
- `isolation.level=read_committed`
- Verificação de idempotência antes do processamento
- Retry com backoff exponencial
- Dead Letter Queue após 3 tentativas

## 🔧 Desenvolvimento

### Estrutura do Projeto

```
src/main/java/com/example/kafka/
├── config/           # Configurações (Kafka, Swagger)
├── controller/       # Endpoints REST
├── dto/              # Objetos de transferência de dados
├── exception/        # Tratamento global de exceções
├── listener/         # Consumidores Kafka
├── producer/         # Produtores Kafka
└── service/          # Regras de negócio
```

### Adicionando Novas Funcionalidades

1. **Novos DTOs**: Adicione em `dto/`
2. **Endpoints**: Estenda `VendaController`
3. **Processamento**: Implemente em `service/`
4. **Documentação**: Use anotações Swagger

### Testes

```bash
# Executa todos os testes
mvn test

# Executa com perfil de teste
mvn test -Dspring.profiles.active=test
```

## 🚀 Deploy

### Build para Produção

```bash
# Gera JAR otimizado
mvn clean package -Dmaven.test.skip=true

# JAR gerado em:
target/kafka-exercices-0.0.1-SNAPSHOT.jar
```

### Configurações de Produção

- **Kafka Cluster**: Configure múltiplos brokers
- **Idempotência**: Use banco de dados distribuído (Redis/PostgreSQL)
- **Monitoramento**: Integrate Prometheus/Grafana
- **Logs**: Configure agregação centralizada
- **Segurança**: Configure SASL/SSL

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Crie um Pull Request

## 📝 License

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

## 📞 Suporte

- **Documentação**: http://localhost:8080/swagger-ui.html
- **Issues**: Use o sistema de issues do GitHub
- **Email**: dev@empresa.com

**Desenvolvido com ❤️ usando Spring Boot e Apache Kafka**
