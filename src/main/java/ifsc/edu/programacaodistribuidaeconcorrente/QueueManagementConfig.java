package ifsc.edu.programacaodistribuidaeconcorrente;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class QueueManagementConfig {

    // Spring Boot já configura automaticamente o ConnectionFactory
    // baseado nas propriedades spring.rabbitmq.*
    @Getter
    @Autowired
    private ConnectionFactory connectionFactory;

    @Getter
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Getter
    @Lazy
    private RabbitAdmin rabbitAdmin;

    // Configurações de filas e exchanges
    @Getter
    @Value("${queue.message.name}")
    private String queueName;

    @Value("${queue.message.durable}")
    private boolean queueDurable;

    @Getter
    @Value("${queue.message.routing-key}")
    private String routingKey;

    @Getter
    @Value("${exchange.message.name}")
    private String exchangeName;

    @Value("${exchange.message.type}")
    @Getter
    private String exchangeType;

    @PostConstruct
    public void postConstruct() {
        log.info("QueueManagementConfig inicializado pelo Spring");
        log.info("Queue: " + queueName);
        log.info("Exchange: " + exchangeName);
        log.info("Routing Key: " + routingKey);
    }

    // Beans para configuração automática das filas
    @Bean
    public Queue messageQueue() {
        return QueueBuilder.durable(queueName)
                .withArgument("x-message-ttl", 60000)
                .build();
    }

    @Bean
    public Exchange messageExchange() {
        switch (exchangeType.toLowerCase()) {
            case "direct":
                return new DirectExchange(exchangeName, true, false);
            case "topic":
                return new TopicExchange(exchangeName, true, false);
            case "fanout":
                return new FanoutExchange(exchangeName, true, false);
            case "headers":
                return new HeadersExchange(exchangeName, true, false);
            default:
                throw new IllegalArgumentException("Tipo de exchange inválido: " + exchangeType);
        }
    }

    @Bean
    public Binding messageBinding() {
        return BindingBuilder
                .bind(messageQueue())
                .to((DirectExchange) messageExchange())
                .with(routingKey);
    }

    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(connectionFactory);
    }

    public void init() {
        try {
            log.info("=== Inicializando RabbitMQ ===");

            // Testar conexão
            testConnection();

            log.info("✅ RabbitMQ configurado com sucesso!");
            log.info("Exchange: " + exchangeName + " (tipo: " + exchangeType + ")");
            log.info("Queue: " + queueName + " (durável: " + queueDurable + ")");
            log.info("Binding: " + routingKey);

        } catch (Exception e) {
            log.error("❌ Erro ao configurar RabbitMQ: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Falha na configuração do RabbitMQ", e);
        }
    }

    private void testConnection() {
        try {
            // Teste simples usando RabbitTemplate
            rabbitTemplate.execute(channel -> {
                log.info("✅ Conexão RabbitMQ testada com sucesso");
                log.info("Canal ativo: " + channel.isOpen());
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Falha ao testar conexão RabbitMQ", e);
        }
    }

    // Métodos utilitários para mensagens
    public void sendMessage(String message) {
        try {
            rabbitTemplate.convertAndSend(exchangeName, routingKey, message);
            log.info("📤 Mensagem enviada: " + message);
        } catch (Exception e) {
            log.error("❌ Erro ao enviar mensagem: " + e.getMessage());
            throw e;
        }
    }

    public String receiveMessage() {
        try {
            Object message = rabbitTemplate.receiveAndConvert(queueName);
            if (message != null) {
                log.info("📥 Mensagem recebida: " + message);
                return message.toString();
            }
            return null;
        } catch (Exception e) {
            log.error("❌ Erro ao receber mensagem: " + e.getMessage());
            throw e;
        }
    }

}