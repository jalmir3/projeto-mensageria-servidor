package ifsc.edu.programacaodistribuidaeconcorrente.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import ifsc.edu.programacaodistribuidaeconcorrente.model.Message;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
public class MessageRepository {

    private final CqlSession session;

    @PostConstruct
    public void init() {
        session.execute("CREATE TABLE IF NOT EXISTS messages (" +
                "id UUID PRIMARY KEY, " +
                "sender TEXT, " +
                "recipient TEXT, " +
                "content TEXT, " +
                "timestamp BIGINT)");
    }

    public void saveMessage(Message message) {
        session.execute("INSERT INTO messages (id, sender, recipient, content, timestamp) " +
                        "VALUES (?, ?, ?, ?, ?)",
                message.getId(),
                message.getSender(),
                message.getRecipient(),
                message.getContent(),
                message.getTimestamp());
    }

    public List<Message> getMessagesByRecipient(String recipient) {
        return session.execute("SELECT * FROM messages WHERE recipient = ? ALLOW FILTERING", recipient)
                .all().stream()
                .map(row -> new Message(
                        row.getUuid("id"),
                        row.getString("sender"),
                        row.getString("recipient"),
                        row.getString("content"),
                        row.getLong("timestamp")))
                .collect(Collectors.toList());
    }

    public List<Message> getAllMessages() {
        return session.execute("SELECT * FROM messages")
                .all().stream()
                .map(row -> new Message(
                        row.getUuid("id"),
                        row.getString("sender"),
                        row.getString("recipient"),
                        row.getString("content"),
                        row.getLong("timestamp")))
                .collect(Collectors.toList());
    }
}