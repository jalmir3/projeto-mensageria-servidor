package ifsc.edu.programacaodistribuidaeconcorrente.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PresenceRabbitConfig {
    
    public static final String PRESENCE_EXCHANGE = "presence-exchange";
    public static final String PRESENCE_QUEUE = "presence-events";
    
    @Bean
    public FanoutExchange presenceExchange() {
        return new FanoutExchange(PRESENCE_EXCHANGE, true, false);
    }
    
    @Bean
    public Queue presenceQueue() {
        return QueueBuilder.durable(PRESENCE_QUEUE).build();
    }
    
    @Bean
    public Binding presenceBinding() {
        return BindingBuilder
                .bind(presenceQueue())
                .to(presenceExchange());
    }
    
    // If you want to use anonymous queues for each instance:
    @Bean
    public Queue anonymousQueue() {
        return new AnonymousQueue();
    }
    
    @Bean
    public Binding anonymousBinding() {
        return BindingBuilder
                .bind(anonymousQueue())
                .to(presenceExchange());
    }
}