package ifsc.edu.programacaodistribuidaeconcorrente;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Repository
public class MessageRepository {

    private final CqlSession session;

    @Autowired
    public MessageRepository(CqlSession session) {
        this.session = session;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            // Verifica se a tabela já existe
            boolean tableExists = session.execute(
                    "SELECT table_name FROM system_schema.tables " +
                            "WHERE keyspace_name = 'message_system' AND table_name = 'messages'"
            ).one() != null;

            if (!tableExists) {
                String createTableQuery = "CREATE TABLE messages (" +
                        "id UUID PRIMARY KEY, " +
                        "sender TEXT, " +
                        "content TEXT, " +
                        "timestamp BIGINT, " +
                        "status TEXT)";

                session.execute(createTableQuery);
                log.info("✅ Tabela 'messages' criada no Cassandra");
            } else {
                // Verifica se a estrutura está correta
                verifyTableStructure();
                log.info("✅ Tabela 'messages' já existe");
            }
        } catch (Exception e) {
            log.error("❌ Erro ao criar/verificar tabela no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao criar tabela", e);
        }
    }

    private void verifyTableStructure() {
        try {
            // Tenta uma operação simples para verificar a estrutura
            session.execute("SELECT id, sender, content, timestamp, status FROM messages LIMIT 1");
        } catch (Exception e) {
            log.error("Estrutura da tabela incorreta. Recriando tabela...");
            session.execute("DROP TABLE IF EXISTS messages");
            createTableIfNotExists();
        }
    }

    public void saveMessage(Message message) {
        try {
            String insertQuery = "INSERT INTO messages (id, sender, content, timestamp, status) " +
                    "VALUES (?, ?, ?, ?, ?)";

            PreparedStatement prepared = session.prepare(insertQuery);
            BoundStatement bound = prepared.bind(
                    message.getId(),
                    message.getSender(),
                    message.getContent(),
                    message.getTimestamp(),
                    message.getStatus()
            );

            session.execute(bound);
            log.debug("Mensagem salva no Cassandra: {}", message.getId());
        } catch (Exception e) {
            log.error("Erro ao salvar mensagem no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao salvar mensagem", e);
        }
    }

    public List<Message> getMessages() {
        List<Message> messages = new ArrayList<>();
        try {
            ResultSet resultSet = session.execute("SELECT id, sender, content, timestamp, status FROM messages");

            for (Row row : resultSet) {
                messages.add(new Message(
                        row.getUuid("id"),
                        row.getString("sender"),
                        row.getString("receiver"),
                        row.getString("content"),
                        row.getLong("timestamp"),
                        row.getString("status"),
                        row.getBoolean("delivered")
                ));
            }
            log.debug("Recuperadas {} mensagens do Cassandra", messages.size());
        } catch (Exception e) {
            log.error("Erro ao recuperar mensagens do Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens", e);
        }
        return messages;
    }

    public void updateDeliveryStatus(UUID id) {
        try {
            String updateQuery = "UPDATE messages SET delivered = true WHERE id = ?";
            PreparedStatement prepared = session.prepare(updateQuery);
            BoundStatement bound = prepared.bind(id);
            
            session.execute(bound);
            log.debug("Status de entrega atualizado para mensagem: {}", id);
        } catch (Exception e) {
            log.error("Erro ao atualizar status de entrega: " + e.getMessage());
            throw new RuntimeException("Falha ao atualizar status de entrega", e);
        }
    }

    public List<Message> getMessagesByReceiverId(String receiverId) {
        List<Message> messages = new ArrayList<>();
        try {
            String selectQuery = "SELECT id, sender, receiver, content, timestamp, status, delivered FROM messages WHERE receiver = ? ALLOW FILTERING";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(receiverId);
            
            ResultSet resultSet = session.execute(bound);

            for (Row row : resultSet) {
                messages.add(new Message(
                        row.getUuid("id"),
                        row.getString("sender"),
                        row.getString("receiver"),
                        row.getString("content"),
                        row.getLong("timestamp"),
                        row.getString("status"),
                        row.getBoolean("delivered")
                ));
            }
            log.debug("Recuperadas {} mensagens para o receptor: {}", messages.size(), receiverId);
        } catch (Exception e) {
            log.error("Erro ao recuperar mensagens por receptor: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens por receptor", e);
        }
        return messages;
    }
}