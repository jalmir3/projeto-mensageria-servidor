package ifsc.edu.programacaodistribuidaeconcorrente;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api")
public class SimpleMessageController {

    private final RabbitTemplate rabbitTemplate;
    private final MessageRepository messageRepository;
    private final String exchangeName;
    private final String routingKey;

    @Autowired
    public SimpleMessageController(RabbitTemplate rabbitTemplate,
                                   MessageRepository messageRepository,
                                   @Value("${exchange.message.name}") String exchangeName,
                                   @Value("${queue.message.routing-key}") String routingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.messageRepository = messageRepository;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody Map<String, Object> request) {
        try {
            Message message = new Message(
                    UUID.randomUUID(),
                    request.getOrDefault("sender", "Anônimo").toString(),
                    request.get("message").toString(),
                    System.currentTimeMillis(),
                    "sent"
            );

            // Envia para RabbitMQ
            rabbitTemplate.convertAndSend(exchangeName, routingKey, message);
            log.info("📤 Mensagem enviada para RabbitMQ: {}", message);

            // Salva no Cassandra
            messageRepository.saveMessage(message);
            log.info("💾 Mensagem salva no Cassandra: {}", message.getId());

            return ResponseEntity.ok(Map.of(
                    "status", "sent",
                    "message", "Mensagem enviada com sucesso!",
                    "message_id", message.getId().toString()
            ));
        } catch (Exception e) {
            log.error("❌ Erro ao processar mensagem: {}", e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", "Falha ao processar mensagem",
                    "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/receive")
    public ResponseEntity<Map<String, Object>> receiveMessages() {
        try {
            List<Message> messages = messageRepository.getMessages();
            return ResponseEntity.ok(Map.of(
                    "status", "ok",
                    "messages", messages.stream().map(this::convertToMap).collect(Collectors.toList()),
                    "count", messages.size()
            ));
        } catch (Exception e) {
            log.error("❌ Erro ao recuperar mensagens: {}", e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", "Falha ao recuperar mensagens"
            ));
        }
    }

    private Map<String, Object> convertToMap(Message message) {
        return Map.of(
                "id", message.getId().toString(),
                "sender", message.getSender(),
                "message", message.getContent(),
                "timestamp", message.getTimestamp(),
                "status", message.getStatus()
        );
    }
}