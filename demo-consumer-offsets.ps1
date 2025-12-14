
Write-Host ""
Write-Host "👀 IMPORTANTE: Note que uma nova mensagem foi enviada para Kafka," -ForegroundColor Yellow
Write-Host "mas o processamento foi rejeitado por duplicata!" -ForegroundColor Yellow
Write-Host "- O offset em __consumer_offsets avança (mensagem foi consumida)"
Write-Host "- Mas o cache de idempotência não muda (venda não foi reprocessada)"
Pause-Demo

Write-Host "📊 STEP 7: Verificando estatísticas finais" -ForegroundColor Blue
Write-Host "Quantas vendas únicas foram processadas..."
$response = Invoke-ApiCall "http://localhost:8080/api/v1/monitoring/processing-stats"
Write-Host $response
Pause-Demo

Write-Host "🛍️ STEP 8: Enviando múltiplas vendas rapidamente" -ForegroundColor Green
Write-Host "Enviando 3 vendas em sequência..."

for ($i = 100003; $i -le 100005; $i++) {
    Write-Host "Enviando venda $i..."
    $bodyBatch = @{
        pedidoId = $i
        produto = "Produto Batch $i"
        valor = 100.00
        quantidade = 1
        emailCliente = "batch$i@example.com"
    } | ConvertTo-Json

    $response = Invoke-ApiCall "http://localhost:8080/api/v1/vendas" "POST" $bodyBatch
    Write-Host $response
    Start-Sleep 1
}
Pause-Demo

Write-Host "🔍 STEP 9: Simulação de como funcionam os commits" -ForegroundColor Blue
$response = Invoke-ApiCall "http://localhost:8080/api/v1/monitoring/offset-commit-simulation"
Write-Host $response
Pause-Demo

Write-Host "📖 STEP 10: Guia de debugging" -ForegroundColor Yellow
Write-Host "Como debuggar problemas com offsets..."
$response = Invoke-ApiCall "http://localhost:8080/api/v1/monitoring/debugging-guide"
Write-Host $response
Pause-Demo

Write-Host "✅ DEMONSTRAÇÃO CONCLUÍDA!" -ForegroundColor Green
Write-Host ""
Write-Host "🎯 O que você deve ter observado:" -ForegroundColor White
Write-Host ""
Write-Host "1. 📊 No __consumer_offsets (via Kafdrop):"
Write-Host "   - Novas entradas a cada venda processada"
Write-Host "   - Offset incrementando sequencialmente"
Write-Host "   - Key com format: {group, topic, partition}"
Write-Host ""
Write-Host "2. 🔄 Na aplicação:"
Write-Host "   - Logs de processamento para vendas únicas"
Write-Host "   - Rejeição de duplicatas (mesma pedidoId)"
Write-Host "   - Increment do cache de idempotência apenas para vendas novas"
Write-Host ""
Write-Host "3. 🧠 Conceitos importantes:"
Write-Host "   - __consumer_offsets rastreia CONSUMO de mensagens"
Write-Host "   - Idempotency Service rastreia PROCESSAMENTO de vendas"
Write-Host "   - São coisas diferentes mas complementares!"
Write-Host ""
Write-Host "🔗 Links úteis:" -ForegroundColor Blue
Write-Host "- Kafdrop: http://localhost:9000"
Write-Host "- API Docs: http://localhost:8080/swagger-ui.html"
Write-Host "- Monitoring: http://localhost:8080/api/v1/monitoring/"
Write-Host ""
Write-Host "🎉 Agora você entende como funciona o __consumer_offsets!" -ForegroundColor Green
