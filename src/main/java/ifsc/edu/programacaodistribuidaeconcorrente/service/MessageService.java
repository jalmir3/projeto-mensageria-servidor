package ifsc.edu.programacaodistribuidaeconcorrente.service;

import ifsc.edu.programacaodistribuidaeconcorrente.model.Message;
import ifsc.edu.programacaodistribuidaeconcorrente.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MessageService {
    private static final String DEFAULT_SENDER = "Anonymous";
    private static final String DEFAULT_CONTENT = "";

    private final RabbitTemplate rabbitTemplate;
    private final MessageRepository messageRepository;

    public Map<String, String> sendMessageTo(String sender, String recipient, String content) {
        try {
            Message message = new Message(
                    UUID.randomUUID(),
                    Objects.requireNonNullElse(sender, DEFAULT_SENDER),
                    Objects.requireNonNull(recipient, "Recipient cannot be null"),
                    Objects.requireNonNullElse(content, DEFAULT_CONTENT),
                    System.currentTimeMillis()
            );

            rabbitTemplate.convertAndSend("message-exchange", "message.routing", message);
            messageRepository.saveMessage(message);

            return Map.of(
                    "status", "sent",
                    "message_id", message.getId().toString()
            );
        } catch (Exception e) {
            return Map.of(
                    "status", "error",
                    "message", e.getMessage()
            );
        }
    }

    public List<Map<String, Object>> getMessagesForRecipient(String recipient) {
        Objects.requireNonNull(recipient, "Recipient cannot be null");
        return messageRepository.getMessagesByRecipient(recipient).stream()
                .map(this::convertToMap)
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getAllMessages() {
        return messageRepository.getAllMessages().stream()
                .map(this::convertToMap)
                .collect(Collectors.toList());
    }

    private Map<String, Object> convertToMap(Message msg) {
        if (msg == null) return Map.of();

        return new LinkedHashMap<>() {{
            put("id", msg.getId() != null ? msg.getId().toString() : "");
            put("sender", msg.getSender() != null ? msg.getSender() : DEFAULT_SENDER);
            put("recipient", msg.getRecipient() != null ? msg.getRecipient() : "");
            put("message", msg.getContent() != null ? msg.getContent() : DEFAULT_CONTENT);
            put("timestamp", msg.getTimestamp());
        }};
    }
}