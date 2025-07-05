package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqUserSetupService {

    private final AmqpAdmin amqpAdmin;

    public RabbitMqUserSetupService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    public void createUserQueue(String userId) {
        String queueName = "messages.user." + userId;
        String exchangeName = "messages.direct";
        String routingKey = "user." + userId;

        Queue queue = new Queue(queueName, true);
        amqpAdmin.declareQueue(queue);

        DirectExchange exchange = new DirectExchange(exchangeName);
        amqpAdmin.declareExchange(exchange);

        Binding binding = BindingBuilder.bind(queue).to(exchange).with(routingKey);
        amqpAdmin.declareBinding(binding);
    }

    public void createDeviceQueue(String userId, String deviceId) {
        String queueName = "messages.user." + userId + ".device." + deviceId;
        String routingKey = "user." + userId + ".device." + deviceId;

        Queue queue = new Queue(queueName, true);
        amqpAdmin.declareQueue(queue);

        TopicExchange exchange = new TopicExchange(RabbitMQConfig.EXCHANGE_NAME);
        amqpAdmin.declareExchange(exchange);

        Binding binding = BindingBuilder.bind(queue).to(exchange).with(routingKey);
        amqpAdmin.declareBinding(binding);
    }
}
