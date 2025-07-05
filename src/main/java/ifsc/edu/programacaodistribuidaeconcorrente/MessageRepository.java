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
                        row.getString("content"),
                        row.getLong("timestamp"),
                        row.getString("status")
                ));
            }
            log.debug("Recuperadas {} mensagens do Cassandra", messages.size());
        } catch (Exception e) {
            log.error("Erro ao recuperar mensagens do Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens", e);
        }
        return messages;
    }
}