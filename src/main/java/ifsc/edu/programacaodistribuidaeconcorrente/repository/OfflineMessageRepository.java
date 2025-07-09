package ifsc.edu.programacaodistribuidaeconcorrente.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import ifsc.edu.programacaodistribuidaeconcorrente.model.OfflineMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class OfflineMessageRepository {

    private final CqlSession session;

    @Autowired
    public OfflineMessageRepository(CqlSession session) {
        this.session = session;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            // Verifica se a tabela já existe
            boolean tableExists = session.execute(
                    "SELECT table_name FROM system_schema.tables " +
                            "WHERE keyspace_name = 'message_system' AND table_name = 'offline_messages'"
            ).one() != null;

            if (!tableExists) {
                String createTableQuery = "CREATE TABLE offline_messages (" +
                        "user_id TEXT, " +
                        "device_id TEXT, " +
                        "created_at TIMESTAMP, " +
                        "message_id UUID, " +
                        "sender_id TEXT, " +
                        "content TEXT, " +
                        "PRIMARY KEY ((user_id, device_id), created_at))";

                session.execute(createTableQuery);
                log.info("✅ Tabela 'offline_messages' criada no Cassandra");
            } else {
                // Verifica se a estrutura está correta
                verifyTableStructure();
                log.info("✅ Tabela 'offline_messages' já existe");
            }
        } catch (Exception e) {
            log.error("❌ Erro ao criar/verificar tabela no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao criar tabela", e);
        }
    }

    private void verifyTableStructure() {
        try {
            // Tenta uma operação simples para verificar a estrutura
            session.execute("SELECT user_id, device_id, created_at, message_id, sender_id, content FROM offline_messages LIMIT 1");
        } catch (Exception e) {
            log.error("Estrutura da tabela incorreta. Recriando tabela...");
            session.execute("DROP TABLE IF EXISTS offline_messages");
            createTableIfNotExists();
        }
    }

    public void saveOfflineMessage(OfflineMessage offlineMessage) {
        try {
            if (offlineMessage.getKey().getCreatedAt() == null) {
                offlineMessage.getKey().setCreatedAt(Instant.now());
            }

            String insertQuery = "INSERT INTO offline_messages (user_id, device_id, created_at, message_id, sender_id, content) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

            PreparedStatement prepared = session.prepare(insertQuery);
            BoundStatement bound = prepared.bind(
                    offlineMessage.getKey().getUserId(),
                    offlineMessage.getKey().getDeviceId(),
                    offlineMessage.getKey().getCreatedAt(),
                    offlineMessage.getMessageId(),
                    offlineMessage.getSenderId(),
                    offlineMessage.getContent()
            );

            session.execute(bound);
            log.debug("Mensagem offline salva no Cassandra: {} para usuário {}", 
                    offlineMessage.getMessageId(), offlineMessage.getKey().getUserId());
        } catch (Exception e) {
            log.error("Erro ao salvar mensagem offline no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao salvar mensagem offline", e);
        }
    }

    public OfflineMessage findByKey(OfflineMessage.Key key) {
        try {
            String selectQuery = "SELECT user_id, device_id, created_at, message_id, sender_id, content " +
                    "FROM offline_messages WHERE user_id = ? AND device_id = ? AND created_at = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(key.getUserId(), key.getDeviceId(), key.getCreatedAt());
            
            ResultSet resultSet = session.execute(bound);
            Row row = resultSet.one();
            
            if (row == null) {
                log.debug("Mensagem offline não encontrada para chave: {}/{}/{}", 
                        key.getUserId(), key.getDeviceId(), key.getCreatedAt());
                return null;
            }
            
            OfflineMessage offlineMessage = new OfflineMessage();
            OfflineMessage.Key messageKey = new OfflineMessage.Key(
                    row.getString("user_id"),
                    row.getString("device_id"),
                    row.getInstant("created_at")
            );
            
            offlineMessage.setKey(messageKey);
            offlineMessage.setMessageId(row.getUuid("message_id"));
            offlineMessage.setSenderId(row.getString("sender_id"));
            offlineMessage.setContent(row.getString("content"));
            
            log.debug("Mensagem offline encontrada: {}", offlineMessage.getMessageId());
            return offlineMessage;
            
        } catch (Exception e) {
            log.error("Erro ao buscar mensagem offline por chave: " + e.getMessage());
            throw new RuntimeException("Falha ao buscar mensagem offline por chave", e);
        }
    }

    public List<OfflineMessage> findByKeyUserIdAndKeyDeviceId(String userId, String deviceId) {
        List<OfflineMessage> offlineMessages = new ArrayList<>();
        try {
            String selectQuery = "SELECT user_id, device_id, created_at, message_id, sender_id, content " +
                    "FROM offline_messages WHERE user_id = ? AND device_id = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(userId, deviceId);
            
            ResultSet resultSet = session.execute(bound);

            for (Row row : resultSet) {
                OfflineMessage offlineMessage = new OfflineMessage();
                OfflineMessage.Key messageKey = new OfflineMessage.Key(
                        row.getString("user_id"),
                        row.getString("device_id"),
                        row.getInstant("created_at")
                );
                
                offlineMessage.setKey(messageKey);
                offlineMessage.setMessageId(row.getUuid("message_id"));
                offlineMessage.setSenderId(row.getString("sender_id"));
                offlineMessage.setContent(row.getString("content"));
                
                offlineMessages.add(offlineMessage);
            }
            
            log.debug("Recuperadas {} mensagens offline para usuário: {} e dispositivo: {}", 
                    offlineMessages.size(), userId, deviceId);
        } catch (Exception e) {
            log.error("Erro ao recuperar mensagens offline por usuário e dispositivo: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens offline", e);
        }
        return offlineMessages;
    }

    public List<OfflineMessage> findByKeyUserId(String userId) {
        List<OfflineMessage> offlineMessages = new ArrayList<>();
        try {
            String selectQuery = "SELECT user_id, device_id, created_at, message_id, sender_id, content " +
                    "FROM offline_messages WHERE user_id = ? ALLOW FILTERING";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(userId);
            
            ResultSet resultSet = session.execute(bound);

            for (Row row : resultSet) {
                OfflineMessage offlineMessage = new OfflineMessage();
                OfflineMessage.Key messageKey = new OfflineMessage.Key(
                        row.getString("user_id"),
                        row.getString("device_id"),
                        row.getInstant("created_at")
                );
                
                offlineMessage.setKey(messageKey);
                offlineMessage.setMessageId(row.getUuid("message_id"));
                offlineMessage.setSenderId(row.getString("sender_id"));
                offlineMessage.setContent(row.getString("content"));
                
                offlineMessages.add(offlineMessage);
            }
            
            log.debug("Recuperadas {} mensagens offline para usuário: {}", offlineMessages.size(), userId);
        } catch (Exception e) {
            log.error("Erro ao recuperar mensagens offline por usuário: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens offline por usuário", e);
        }
        return offlineMessages;
    }

    public void deleteOfflineMessage(OfflineMessage.Key key) {
        try {
            String deleteQuery = "DELETE FROM offline_messages WHERE user_id = ? AND device_id = ? AND created_at = ?";
            PreparedStatement prepared = session.prepare(deleteQuery);
            BoundStatement bound = prepared.bind(key.getUserId(), key.getDeviceId(), key.getCreatedAt());
            
            session.execute(bound);
            log.debug("Mensagem offline deletada: {}/{}/{}", 
                    key.getUserId(), key.getDeviceId(), key.getCreatedAt());
        } catch (Exception e) {
            log.error("Erro ao deletar mensagem offline: " + e.getMessage());
            throw new RuntimeException("Falha ao deletar mensagem offline", e);
        }
    }

    public void deleteByKeyUserIdAndKeyDeviceId(String userId, String deviceId) {
        try {
            String deleteQuery = "DELETE FROM offline_messages WHERE user_id = ? AND device_id = ?";
            PreparedStatement prepared = session.prepare(deleteQuery);
            BoundStatement bound = prepared.bind(userId, deviceId);
            
            session.execute(bound);
            log.debug("Mensagens offline deletadas para usuário: {} e dispositivo: {}", userId, deviceId);
        } catch (Exception e) {
            log.error("Erro ao deletar mensagens offline por usuário e dispositivo: " + e.getMessage());
            throw new RuntimeException("Falha ao deletar mensagens offline", e);
        }
    }

    public List<OfflineMessage> getAllOfflineMessages() {
        List<OfflineMessage> offlineMessages = new ArrayList<>();
        try {
            ResultSet resultSet = session.execute("SELECT user_id, device_id, created_at, message_id, sender_id, content FROM offline_messages");

            for (Row row : resultSet) {
                OfflineMessage offlineMessage = new OfflineMessage();
                OfflineMessage.Key messageKey = new OfflineMessage.Key(
                        row.getString("user_id"),
                        row.getString("device_id"),
                        row.getInstant("created_at")
                );
                
                offlineMessage.setKey(messageKey);
                offlineMessage.setMessageId(row.getUuid("message_id"));
                offlineMessage.setSenderId(row.getString("sender_id"));
                offlineMessage.setContent(row.getString("content"));
                
                offlineMessages.add(offlineMessage);
            }
            
            log.debug("Recuperadas {} mensagens offline do Cassandra", offlineMessages.size());
        } catch (Exception e) {
            log.error("Erro ao recuperar todas as mensagens offline do Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar mensagens offline", e);
        }
        return offlineMessages;
    }
}