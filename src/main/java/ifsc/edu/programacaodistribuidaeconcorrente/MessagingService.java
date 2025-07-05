package ifsc.edu.programacaodistribuidaeconcorrente;
import java.util.UUID;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MessagingService {

    private final RabbitTemplate rabbitTemplate;
    private final MessageRepository messageRepository;

    @Value("${rabbitmq.exchange}")
    private String exchange;

    public MessagingService(RabbitTemplate rabbitTemplate, MessageRepository messageRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.messageRepository = messageRepository;
    }

    public UUID sendMessage(String senderId, String receiverId, String content) {
        UUID messageId = UUID.randomUUID();
        Message message = new Message(
            messageId,
            senderId,
            receiverId,
            content,
            System.currentTimeMillis(),
            "sent",
            false
        );
        messageRepository.saveMessage(message);

        rabbitTemplate.convertAndSend(
            exchange, 
            "user." + message.getReceiver(), 
            message
        );

        return message.getId();
    }

    public void markDelivered(UUID messageId) {
        Message message = messageRepository.findById(messageId);
        if (message != null){
            message.setDelivered(true);
            messageRepository.saveMessage(message);
        }
    }
}
