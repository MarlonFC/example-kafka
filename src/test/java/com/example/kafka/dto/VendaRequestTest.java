package com.example.kafka.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Testes unitários para VendaRequest.
 *
 * Exemplifica:
 * - Testes de validação Bean Validation (JSR-303)
 * - Testes de serialização/deserialização JSON
 * - Testes de equals, hashCode e toString
 * - Testes parametrizados para diferentes cenários
 * - Validação de constraints customizadas
 */
@DisplayName("Testes para VendaRequest")
class VendaRequestTest {

    private Validator validator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("Testes de validação")
    class ValidacaoTests {

        @Test
        @DisplayName("Deve validar venda com todos os campos válidos")
        void deveValidarVendaComTodosCamposValidos() {
            // Arrange
            VendaRequest venda = criarVendaValida();

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).isEmpty();
        }

        @Test
        @DisplayName("Deve falhar com pedidoId null")
        void deveFalharComPedidoIdNull() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setPedidoId(null);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            ConstraintViolation<VendaRequest> violation = violations.iterator().next();
            assertThat(violation.getMessage()).isEqualTo("ID do pedido é obrigatório");
            assertThat(violation.getPropertyPath().toString()).isEqualTo("pedidoId");
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"  ", "\t", "\n"})
        @DisplayName("Deve falhar com produto inválido")
        void deveFalharComProdutoInvalido(String produto) {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setProduto(produto);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            ConstraintViolation<VendaRequest> violation = violations.iterator().next();
            assertThat(violation.getMessage()).isEqualTo("Nome do produto é obrigatório");
        }

        @Test
        @DisplayName("Deve falhar com valor null")
        void deveFalharComValorNull() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setValor(null);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            assertThat(violations.iterator().next().getMessage()).isEqualTo("Valor é obrigatório");
        }

        @ParameterizedTest
        @ValueSource(strings = {"-1", "-0.01", "-100.00", "0"})
        @DisplayName("Deve falhar com valor não positivo")
        void deveFalharComValorNaoPositivo(String valorString) {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setValor(new BigDecimal(valorString));

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            assertThat(violations.iterator().next().getMessage()).isEqualTo("Valor deve ser positivo");
        }

        @Test
        @DisplayName("Deve falhar com quantidade null")
        void deveFalharComQuantidadeNull() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setQuantidade(null);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            assertThat(violations.iterator().next().getMessage()).isEqualTo("Quantidade é obrigatória");
        }

        @ParameterizedTest
        @ValueSource(ints = {-1, -10, 0})
        @DisplayName("Deve falhar com quantidade não positiva")
        void deveFalharComQuantidadeNaoPositiva(int quantidade) {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setQuantidade(quantidade);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            assertThat(violations.iterator().next().getMessage()).isEqualTo("Quantidade deve ser positiva");
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"  ", "\t", "\n"})
        @DisplayName("Deve falhar com email inválido")
        void deveFalharComEmailInvalido(String email) {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setEmailCliente(email);

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(1);
            assertThat(violations.iterator().next().getMessage()).isEqualTo("Email do cliente é obrigatório");
        }

        @Test
        @DisplayName("Deve falhar com múltiplos campos inválidos")
        void deveFalharComMultiplosCamposInvalidos() {
            // Arrange
            VendaRequest venda = new VendaRequest();
            venda.setPedidoId(null);
            venda.setProduto("");
            venda.setValor(null);
            venda.setQuantidade(null);
            venda.setEmailCliente("");

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).hasSize(5); // Uma violação para cada campo
        }
    }

    @Nested
    @DisplayName("Testes de serialização JSON")
    class SerializacaoJsonTests {

        @Test
        @DisplayName("Deve serializar para JSON corretamente")
        void deveSerializarParaJsonCorretamente() throws Exception {
            // Arrange
            VendaRequest venda = criarVendaValida();

            // Act
            String json = objectMapper.writeValueAsString(venda);

            // Assert
            assertThat(json).contains("\"pedidoId\":12345");
            assertThat(json).contains("\"produto\":\"Notebook Dell Inspiron\"");
            assertThat(json).contains("\"valor\":2500.99");
            assertThat(json).contains("\"quantidade\":2");
            assertThat(json).contains("\"emailCliente\":\"cliente@email.com\"");
        }

        @Test
        @DisplayName("Deve deserializar de JSON corretamente")
        void deveDeserializarDeJsonCorretamente() throws Exception {
            // Arrange
            String json = "{\n" +
                "  \"pedidoId\": 12345,\n" +
                "  \"produto\": \"Notebook Dell Inspiron\",\n" +
                "  \"valor\": 2500.99,\n" +
                "  \"quantidade\": 2,\n" +
                "  \"emailCliente\": \"cliente@email.com\"\n" +
                "}";

            // Act
            VendaRequest venda = objectMapper.readValue(json, VendaRequest.class);

            // Assert
            assertThat(venda.getPedidoId()).isEqualTo(12345L);
            assertThat(venda.getProduto()).isEqualTo("Notebook Dell Inspiron");
            assertThat(venda.getValor()).isEqualByComparingTo(new BigDecimal("2500.99"));
            assertThat(venda.getQuantidade()).isEqualTo(2);
            assertThat(venda.getEmailCliente()).isEqualTo("cliente@email.com");
        }

        @Test
        @DisplayName("Deve manter precisão decimal na serialização")
        void deveManterPrecisaoDecimalNaSerializacao() throws Exception {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setValor(new BigDecimal("1234567.89"));

            // Act
            String json = objectMapper.writeValueAsString(venda);
            VendaRequest vendaDesserializada = objectMapper.readValue(json, VendaRequest.class);

            // Assert
            assertThat(vendaDesserializada.getValor()).isEqualByComparingTo(new BigDecimal("1234567.89"));
        }

        @Test
        @DisplayName("Deve tratar caracteres especiais na serialização")
        void deveTratarCaracteresEspeciaisNaSerializacao() throws Exception {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setProduto("Produto com àçêñtôs & símb@los #especiais! 中文 🎉");
            venda.setEmailCliente("usuário.téste+tag@exêmplo.com.br");

            // Act
            String json = objectMapper.writeValueAsString(venda);
            VendaRequest vendaDesserializada = objectMapper.readValue(json, VendaRequest.class);

            // Assert
            assertThat(vendaDesserializada.getProduto()).isEqualTo(venda.getProduto());
            assertThat(vendaDesserializada.getEmailCliente()).isEqualTo(venda.getEmailCliente());
        }

        @Test
        @DisplayName("Deve tratar JSON com campos extras")
        void deveTratarJsonComCamposExtras() throws Exception {
            // Arrange - JSON com campos não mapeados (simulando evolução da API)
            String json = "{\n" +
                "  \"pedidoId\": 12345,\n" +
                "  \"produto\": \"Notebook\",\n" +
                "  \"valor\": 1500.00,\n" +
                "  \"quantidade\": 1,\n" +
                "  \"emailCliente\": \"cliente@email.com\",\n" +
                "  \"campoExtra\": \"valor ignorado\",\n" +
                "  \"outroExtra\": 999,\n" +
                "  \"versaoApi\": \"2.0\",\n" +
                "  \"metadata\": {\n" +
                "    \"origem\": \"mobile\",\n" +
                "    \"timestamp\": \"2023-12-14T10:30:00Z\"\n" +
                "  }\n" +
                "}";

            // Act - Deserialização deve ignorar campos desconhecidos
            VendaRequest venda = objectMapper.readValue(json, VendaRequest.class);

            // Assert - Deve deserializar campos conhecidos normalmente
            assertThat(venda.getPedidoId()).isEqualTo(12345L);
            assertThat(venda.getProduto()).isEqualTo("Notebook");
            assertThat(venda.getValor()).isEqualByComparingTo(new BigDecimal("1500.00"));
            assertThat(venda.getQuantidade()).isEqualTo(1);
            assertThat(venda.getEmailCliente()).isEqualTo("cliente@email.com");

            // Assert adicional - Verifica que objeto é válido após deserialização
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);
            assertThat(violations).isEmpty();
        }

        @Test
        @DisplayName("Deve manter comportamento idempotente na serialização/deserialização")
        void deveManterComportamentoIdempotenteNaSerializacaoDeserializacao() throws Exception {
            // Arrange
            VendaRequest vendaOriginal = criarVendaValida();

            // Act - Ciclo completo: serializa e deserializa
            String json = objectMapper.writeValueAsString(vendaOriginal);
            VendaRequest vendaDeserializada = objectMapper.readValue(json, VendaRequest.class);

            // Assert - Objetos devem ser equivalentes
            assertThat(vendaDeserializada.getPedidoId()).isEqualTo(vendaOriginal.getPedidoId());
            assertThat(vendaDeserializada.getProduto()).isEqualTo(vendaOriginal.getProduto());
            assertThat(vendaDeserializada.getValor()).isEqualByComparingTo(vendaOriginal.getValor());
            assertThat(vendaDeserializada.getQuantidade()).isEqualTo(vendaOriginal.getQuantidade());
            assertThat(vendaDeserializada.getEmailCliente()).isEqualTo(vendaOriginal.getEmailCliente());

            // Assert - JSON não deve conter campos extras
            assertThat(json).doesNotContain("campoExtra");
            assertThat(json).doesNotContain("outroExtra");
        }
    }

    @Nested
    @DisplayName("Testes de métodos Object")
    class MetodosObjectTests {

        @Test
        @DisplayName("Deve implementar toString corretamente")
        void deveImplementarToStringCorretamente() {
            // Arrange
            VendaRequest venda = criarVendaValida();

            // Act
            String toString = venda.toString();

            // Assert
            assertThat(toString).contains("VendaRequest{");
            assertThat(toString).contains("pedidoId=12345");
            assertThat(toString).contains("produto='Notebook Dell Inspiron'");
            assertThat(toString).contains("valor=2500.99");
            assertThat(toString).contains("quantidade=2");
            assertThat(toString).contains("emailCliente='cliente@email.com'");
        }

        @Test
        @DisplayName("Deve funcionar como valor em collections")
        void deveFuncionarComoValorEmCollections() {
            // Arrange
            VendaRequest venda1 = criarVendaValida();
            VendaRequest venda2 = criarVendaValida();

            // Act & Assert - Duas instâncias com mesmos dados devem ser consideradas iguais
            assertThat(venda1).isEqualTo(venda2);
            assertThat(venda1.hashCode()).isEqualTo(venda2.hashCode());

            // Testando em Set (não deve duplicar)
            java.util.Set<VendaRequest> vendas = new java.util.HashSet<>();
            vendas.add(venda1);
            vendas.add(venda2);

            assertThat(vendas).hasSize(1);
        }

        @Test
        @DisplayName("Deve diferir para dados diferentes")
        void deveDiferirParaDadosDiferentes() {
            // Arrange
            VendaRequest venda1 = criarVendaValida();
            VendaRequest venda2 = criarVendaValida();
            venda2.setPedidoId(99999L);

            // Act & Assert
            assertThat(venda1).isNotEqualTo(venda2);
            assertThat(venda1.hashCode()).isNotEqualTo(venda2.hashCode());
        }

        @Test
        @DisplayName("Deve implementar equals() corretamente")
        void deveImplementarEqualsCorretamente() {
            // Arrange
            VendaRequest venda1 = criarVendaValida();
            VendaRequest venda2 = criarVendaValida();
            VendaRequest venda3 = criarVendaValida();
            venda3.setProduto("Produto Diferente");

            // Assert - Reflexivity: x.equals(x) deve ser true
            assertThat(venda1).isEqualTo(venda1);

            // Assert - Symmetry: x.equals(y) == y.equals(x)
            assertThat(venda1.equals(venda2)).isEqualTo(venda2.equals(venda1));

            // Assert - Transitivity: se x.equals(y) e y.equals(z), então x.equals(z)
            VendaRequest vendaZ = criarVendaValida();
            assertThat(venda1).isEqualTo(venda2);
            assertThat(venda2).isEqualTo(vendaZ);
            assertThat(venda1).isEqualTo(vendaZ);

            // Assert - Consistency: múltiplas invocações devem retornar o mesmo resultado
            assertThat(venda1.equals(venda2)).isTrue();
            assertThat(venda1.equals(venda2)).isTrue();

            // Assert - null: x.equals(null) deve ser false
            assertThat(venda1.equals(null)).isFalse();

            // Assert - diferentes objetos devem ser diferentes
            assertThat(venda1).isNotEqualTo(venda3);
        }

        @Test
        @DisplayName("Deve implementar hashCode() corretamente")
        void deveImplementarHashCodeCorretamente() {
            // Arrange
            VendaRequest venda1 = criarVendaValida();
            VendaRequest venda2 = criarVendaValida();
            VendaRequest venda3 = criarVendaValida();
            venda3.setProduto("Produto Diferente");

            // Assert - Se dois objetos são iguais, seus hashCodes devem ser iguais
            assertThat(venda1).isEqualTo(venda2);
            assertThat(venda1.hashCode()).isEqualTo(venda2.hashCode());

            // Assert - Consistency: múltiplas chamadas devem retornar o mesmo valor
            int hash1 = venda1.hashCode();
            int hash2 = venda1.hashCode();
            assertThat(hash1).isEqualTo(hash2);

            // Assert - Objetos diferentes podem ter hashCodes diferentes (não obrigatório, mas desejável)
            assertThat(venda1.hashCode()).isNotEqualTo(venda3.hashCode());
        }

        @Test
        @DisplayName("Deve tratar equals/hashCode com campos null")
        void deveTratarEqualsHashCodeComCamposNull() {
            // Arrange
            VendaRequest venda1 = new VendaRequest();
            VendaRequest venda2 = new VendaRequest();
            VendaRequest venda3 = new VendaRequest();
            venda3.setPedidoId(123L);

            // Act & Assert - Objetos com todos os campos null devem ser iguais
            assertThat(venda1).isEqualTo(venda2);
            assertThat(venda1.hashCode()).isEqualTo(venda2.hashCode());

            // Assert - Objeto com campo null vs objeto com campo preenchido devem ser diferentes
            assertThat(venda1).isNotEqualTo(venda3);
            assertThat(venda1.hashCode()).isNotEqualTo(venda3.hashCode());
        }

        @Test
        @DisplayName("Deve funcionar equals com diferentes tipos")
        void deveFuncionarEqualsComDiferentesTipos() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            String outroObjeto = "Não é VendaRequest";
            Object nulo = null;

            // Act & Assert - Deve retornar false para tipos diferentes
            assertThat(venda.equals(outroObjeto)).isFalse();
            assertThat(venda.equals(nulo)).isFalse();

            // Assert - Deve retornar true para o mesmo objeto
            assertThat(venda.equals(venda)).isTrue();
        }
    }

    @Nested
    @DisplayName("Testes de construtores")
    class ConstrutoresTests {

        @Test
        @DisplayName("Deve funcionar com construtor padrão")
        void deveFuncionarComConstrutorPadrao() {
            // Act
            VendaRequest venda = new VendaRequest();

            // Assert
            assertThat(venda.getPedidoId()).isNull();
            assertThat(venda.getProduto()).isNull();
            assertThat(venda.getValor()).isNull();
            assertThat(venda.getQuantidade()).isNull();
            assertThat(venda.getEmailCliente()).isNull();
        }

        @Test
        @DisplayName("Deve funcionar com construtor completo")
        void deveFuncionarComConstrutorCompleto() {
            // Arrange
            Long pedidoId = 12345L;
            String produto = "Notebook";
            BigDecimal valor = new BigDecimal("2500.99");
            Integer quantidade = 2;
            String email = "cliente@email.com";

            // Act
            VendaRequest venda = new VendaRequest(pedidoId, produto, valor, quantidade, email);

            // Assert
            assertThat(venda.getPedidoId()).isEqualTo(pedidoId);
            assertThat(venda.getProduto()).isEqualTo(produto);
            assertThat(venda.getValor()).isEqualTo(valor);
            assertThat(venda.getQuantidade()).isEqualTo(quantidade);
            assertThat(venda.getEmailCliente()).isEqualTo(email);
        }
    }

    @Nested
    @DisplayName("Testes de casos extremos")
    class CasosExtremosTests {

        @Test
        @DisplayName("Deve aceitar valores extremos válidos")
        void deveAceitarValoresExtremosValidos() {
            // Arrange
            VendaRequest venda = new VendaRequest();
            venda.setPedidoId(Long.MAX_VALUE);
            venda.setProduto("P"); // Produto com 1 char
            venda.setValor(new BigDecimal("0.01")); // Menor valor positivo com 2 decimais
            venda.setQuantidade(1); // Menor quantidade positiva
            venda.setEmailCliente("a@b.co"); // Email mínimo válido

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).isEmpty();
        }

        @Test
        @DisplayName("Deve aceitar produto com caracteres especiais e unicode")
        void deveAceitarProdutoComCaracteresEspeciaisEUnicode() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setProduto("Produto com àçêñtôs & símb@los #especiais! 中文字符 🌟🎉 ñáéíóú");

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).isEmpty();
            assertThat(venda.getProduto()).contains("àçêñtôs");
            assertThat(venda.getProduto()).contains("中文字符");
            assertThat(venda.getProduto()).contains("🌟🎉");
        }

        @Test
        @DisplayName("Deve aceitar email com formato complexo")
        void deveAceitarEmailComFormatoComplexo() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setEmailCliente("usuario.teste+tag123@subdomain.exemplo-empresa.co.uk");

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).isEmpty();
        }

        @Test
        @DisplayName("Deve aceitar valores monetários com alta precisão")
        void deveAceitarValoresMonetariosComAltaPrecisao() {
            // Arrange
            VendaRequest venda = criarVendaValida();
            venda.setValor(new BigDecimal("999999999.99"));

            // Act
            Set<ConstraintViolation<VendaRequest>> violations = validator.validate(venda);

            // Assert
            assertThat(violations).isEmpty();
            assertThat(venda.getValor()).isEqualByComparingTo(new BigDecimal("999999999.99"));
        }
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
