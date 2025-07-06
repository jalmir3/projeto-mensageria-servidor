package ifsc.edu.programacaodistribuidaeconcorrente.controller;

import ifsc.edu.programacaodistribuidaeconcorrente.service.MessageService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@AllArgsConstructor
@RequestMapping("/api")
public class MessageController {

    private final MessageService messageService;

    @PostMapping("/send-to")
    public ResponseEntity<Map<String, String>> sendMessageTo(
            @RequestBody Map<String, Object> request) {
        try {
            String sender = request.getOrDefault("sender", "Anonymous").toString();
            String recipient = request.get("recipient").toString();
            String message = request.get("message").toString();

            return ResponseEntity.ok(
                    messageService.sendMessageTo(sender, recipient, message));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", e.getMessage()));
        }
    }

    @GetMapping("/receive/{recipient}")
    public ResponseEntity<List<Map<String, Object>>> receiveMessagesForRecipient(
            @PathVariable("recipient") String recipient) {
        return ResponseEntity.ok(
                messageService.getMessagesForRecipient(recipient));
    }

    @GetMapping("/receive")
    public ResponseEntity<List<Map<String, Object>>> receiveMessages() {
        return ResponseEntity.ok(
                messageService.getAllMessages());
    }
}