package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicação Spring Boot para processamento de vendas com Apache Kafka.
 *
 * Esta aplicação implementa um sistema de mensageria confiável com as seguintes características:
 * - Exactly-once delivery para garantir que cada venda seja processada uma única vez
 * - Idempotência para proteção contra duplicatas
 * - Dead Letter Queue para tratamento de erros
 * - API REST documentada com Swagger
 * - Monitoramento e métricas via Actuator
 *
 * @author Sistema Kafka
 * @version 1.0
 * @since 2024-12-14
 */
@SpringBootApplication
public class Application {

	/**
	 * Método principal da aplicação.
	 *
	 * @param args argumentos de linha de comando
	 */
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
