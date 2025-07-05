package ifsc.edu.programacaodistribuidaeconcorrente;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final MessagingService messagingService;

    @Autowired
    public MessageController(MessagingService messagingService) {
        this.messagingService = messagingService;
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody Map<String, Object> request) {
        try {
            UUID id = messagingService.sendMessage(
                request.getOrDefault("sender", "Anônimo").toString(),
                request.getOrDefault("receiver", "Anônimo").toString(),
                request.get("message").toString()
            );

            log.info("💾 Mensagem salva no Cassandra: {}", id);
            log.info("📤 Mensagem enviada para RabbitMQ: {}", id);

            return ResponseEntity.ok(Map.of(
                    "status", "sent",
                    "message", "Mensagem enviada com sucesso!",
                    "message_id", request.toString()
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

    /*@GetMapping("/receive")
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
    }*/
}