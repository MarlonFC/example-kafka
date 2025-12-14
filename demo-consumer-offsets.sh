#!/bin/bash

# 🎯 Script de Demonstração do __consumer_offsets
# Este script demonstra como funciona o tópico __consumer_offsets na prática

echo "🚀 Demonstração Prática do __consumer_offsets"
echo "=============================================="
echo ""

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para pausar entre steps
pause() {
    echo ""
    echo -e "${YELLOW}Pressione Enter para continuar...${NC}"
    read -r
    echo ""
}

echo "📋 Pré-requisitos:"
echo "- Aplicação rodando em http://localhost:8080"
echo "- Kafka rodando em localhost:9092"
echo "- Kafdrop disponível em http://localhost:9000"
pause

echo -e "${BLUE}🔍 STEP 1: Verificando status inicial${NC}"
echo "Consultando informações do sistema..."
curl -s http://localhost:8080/api/v1/monitoring/consumer-offsets-info | jq .
pause

echo -e "${BLUE}🔍 STEP 2: Status do Consumer Group${NC}"
echo "Verificando se nosso consumer group está ativo..."
curl -s http://localhost:8080/api/v1/monitoring/consumer-group-status | jq .
pause

echo -e "${BLUE}📊 STEP 3: Estatísticas iniciais de processamento${NC}"
echo "Quantas vendas já foram processadas..."
curl -s http://localhost:8080/api/v1/monitoring/processing-stats | jq .
pause

echo -e "${GREEN}🛍️ STEP 4: Enviando primeira venda${NC}"
echo "Enviando venda com pedidoId=100001..."
curl -X POST http://localhost:8080/api/v1/vendas \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": 100001,
    "produto": "Demonstração Offset 1",
    "valor": 299.99,
    "quantidade": 1,
    "emailCliente": "demo1@example.com"
  }' | jq .

echo ""
echo -e "${YELLOW}👀 AGORA: Acesse Kafdrop e veja o que aconteceu!${NC}"
echo "1. Abra http://localhost:9000"
echo "2. Clique no tópico '__consumer_offsets'"
echo "3. Procure uma nova mensagem com key contendo 'vendas-consumer-group'"
echo "4. Observe o offset commitado"
pause

echo -e "${GREEN}🛍️ STEP 5: Enviando segunda venda${NC}"
echo "Enviando venda com pedidoId=100002..."
curl -X POST http://localhost:8080/api/v1/vendas \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": 100002,
    "produto": "Demonstração Offset 2",
    "valor": 599.99,
    "quantidade": 2,
    "emailCliente": "demo2@example.com"
  }' | jq .

echo ""
echo -e "${YELLOW}👀 AGORA: Veja a segunda entrada no __consumer_offsets!${NC}"
echo "O offset deve ter incrementado de 1 para 2"
pause

echo -e "${RED}🔄 STEP 6: Testando idempotência (venda duplicata)${NC}"
echo "Reenviando a mesma venda (pedidoId=100002)..."
curl -X POST http://localhost:8080/api/v1/vendas \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": 100002,
    "produto": "Demonstração Offset 2",
    "valor": 599.99,
    "quantidade": 2,
    "emailCliente": "demo2@example.com"
  }' | jq .

echo ""
echo -e "${YELLOW}👀 IMPORTANTE: Note que uma nova mensagem foi enviada para Kafka,${NC}"
echo -e "${YELLOW}mas o processamento foi rejeitado por duplicata!${NC}"
echo "- O offset em __consumer_offsets avança (mensagem foi consumida)"
echo "- Mas o cache de idempotência não muda (venda não foi reprocessada)"
pause

echo -e "${BLUE}📊 STEP 7: Verificando estatísticas finais${NC}"
echo "Quantas vendas únicas foram processadas..."
curl -s http://localhost:8080/api/v1/monitoring/processing-stats | jq .
pause

echo -e "${GREEN}🛍️ STEP 8: Enviando múltiplas vendas rapidamente${NC}"
echo "Enviando 3 vendas em sequência..."

for i in {100003..100005}; do
  echo "Enviando venda $i..."
  curl -X POST http://localhost:8080/api/v1/vendas \
    -H "Content-Type: application/json" \
    -d "{
      \"pedidoId\": $i,
      \"produto\": \"Produto Batch $i\",
      \"valor\": 100.00,
      \"quantidade\": 1,
      \"emailCliente\": \"batch$i@example.com\"
    }" | jq .
  sleep 1
done
pause

echo -e "${BLUE}🔍 STEP 9: Simulação de como funcionam os commits${NC}"
curl -s http://localhost:8080/api/v1/monitoring/offset-commit-simulation | jq .
pause

echo -e "${YELLOW}📖 STEP 10: Guia de debugging${NC}"
echo "Como debuggar problemas com offsets..."
curl -s http://localhost:8080/api/v1/monitoring/debugging-guide | jq .
pause

echo -e "${GREEN}✅ DEMONSTRAÇÃO CONCLUÍDA!${NC}"
echo ""
echo "🎯 O que você deve ter observado:"
echo ""
echo "1. 📊 No __consumer_offsets (via Kafdrop):"
echo "   - Novas entradas a cada venda processada"
echo "   - Offset incrementando sequencialmente"
echo "   - Key com format: {group, topic, partition}"
echo ""
echo "2. 🔄 Na aplicação:"
echo "   - Logs de processamento para vendas únicas"
echo "   - Rejeição de duplicatas (mesma pedidoId)"
echo "   - Increment do cache de idempotência apenas para vendas novas"
echo ""
echo "3. 🧠 Conceitos importantes:"
echo "   - __consumer_offsets rastreia CONSUMO de mensagens"
echo "   - Idempotency Service rastreia PROCESSAMENTO de vendas"
echo "   - São coisas diferentes mas complementares!"
echo ""
echo -e "${BLUE}🔗 Links úteis:${NC}"
echo "- Kafdrop: http://localhost:9000"
echo "- API Docs: http://localhost:8080/swagger-ui.html"
echo "- Monitoring: http://localhost:8080/api/v1/monitoring/"
echo ""
echo -e "${GREEN}🎉 Agora você entende como funciona o __consumer_offsets!${NC}"
