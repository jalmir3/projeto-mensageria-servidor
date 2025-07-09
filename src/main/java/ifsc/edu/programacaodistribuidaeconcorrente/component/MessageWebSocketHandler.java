package ifsc.edu.programacaodistribuidaeconcorrente.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import ifsc.edu.programacaodistribuidaeconcorrente.service.MessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageWebSocketHandler extends TextWebSocketHandler {

    private final MessageService messageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String query = session.getUri().getQuery();
        if (query != null && query.startsWith("user=")) {
            String username = query.substring("user=".length());
            sessions.put(username, session);
            log.info("Sessão registrada para: " + username);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        Map<String, String> data = objectMapper.readValue(message.getPayload(), Map.class);
        String sender = data.get("sender");
        String recipient = data.get("recipient");
        String content = data.get("message");

        var response = messageService.sendMessageTo(sender, recipient, content);

        Map<String, Object> messageData = new LinkedHashMap<>();
        messageData.put("sender", sender);
        messageData.put("recipient", recipient);
        messageData.put("message", content);
        messageData.put("timestamp", System.currentTimeMillis());

        String json = objectMapper.writeValueAsString(messageData);

        session.sendMessage(new TextMessage(json));

        WebSocketSession recipientSession = sessions.get(recipient);
        if (recipientSession != null && recipientSession.isOpen()) {
            recipientSession.sendMessage(new TextMessage(json));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.entrySet().removeIf(entry -> entry.getValue().equals(session));
        log.info("Sessão desconectada.");
    }
}
