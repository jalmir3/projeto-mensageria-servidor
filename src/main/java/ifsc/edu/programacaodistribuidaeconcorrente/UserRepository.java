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
import java.util.Optional;

@Slf4j
@Repository
public class UserRepository {

    private final CqlSession session;

    @Autowired
    public UserRepository(CqlSession session) {
        this.session = session;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            // Verifica se a tabela já existe
            boolean tableExists = session.execute(
                    "SELECT table_name FROM system_schema.tables " +
                            "WHERE keyspace_name = 'message_system' AND table_name = 'users'"
            ).one() != null;

            if (!tableExists) {
                String createTableQuery = "CREATE TABLE users (" +
                        "user_id TEXT PRIMARY KEY, " +
                        "name TEXT, " +
                        "registered_at TIMESTAMP)";

                session.execute(createTableQuery);
                log.info("✅ Tabela 'users' criada no Cassandra");
            } else {
                // Verifica se a estrutura está correta
                verifyTableStructure();
                log.info("✅ Tabela 'users' já existe");
            }
        } catch (Exception e) {
            log.error("❌ Erro ao criar/verificar tabela no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao criar tabela", e);
        }
    }

    private void verifyTableStructure() {
        try {
            // Tenta uma operação simples para verificar a estrutura
            session.execute("SELECT user_id, name, registered_at FROM users LIMIT 1");
        } catch (Exception e) {
            log.error("Estrutura da tabela incorreta. Recriando tabela...");
            session.execute("DROP TABLE IF EXISTS users");
            createTableIfNotExists();
        }
    }

    public void save(User user) {
        try {
            String insertQuery = "INSERT INTO users (user_id, name, registered_at) " +
                    "VALUES (?, ?, ?)";

            PreparedStatement prepared = session.prepare(insertQuery);
            BoundStatement bound = prepared.bind(
                    user.getUserId(),
                    user.getName(),
                    user.getRegisteredAt()
            );

            session.execute(bound);
            log.debug("Usuário salvo no Cassandra: {}", user.getUserId());
        } catch (Exception e) {
            log.error("Erro ao salvar usuário no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao salvar usuário", e);
        }
    }

    public Optional<User> findById(String userId) {
        try {
            String selectQuery = "SELECT user_id, name, registered_at FROM users WHERE user_id = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(userId);
            
            ResultSet resultSet = session.execute(bound);
            Row row = resultSet.one();

            if (row != null) {
                User user = new User(
                        row.getString("user_id"),
                        row.getString("name"),
                        row.getInstant("registered_at")
                );
                log.debug("Usuário encontrado: {}", userId);
                return Optional.of(user);
            } else {
                log.debug("Usuário não encontrado: {}", userId);
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Erro ao buscar usuário por ID: " + e.getMessage());
            throw new RuntimeException("Falha ao buscar usuário", e);
        }
    }

    public List<User> findAll() {
        List<User> users = new ArrayList<>();
        try {
            ResultSet resultSet = session.execute("SELECT user_id, name, registered_at FROM users");

            for (Row row : resultSet) {
                users.add(new User(
                        row.getString("user_id"),
                        row.getString("name"),
                        row.getInstant("registered_at")
                ));
            }
            log.debug("Recuperados {} usuários do Cassandra", users.size());
        } catch (Exception e) {
            log.error("Erro ao recuperar usuários do Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar usuários", e);
        }
        return users;
    }

    public boolean existsById(String userId) {
        try {
            String selectQuery = "SELECT user_id FROM users WHERE user_id = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(userId);
            
            ResultSet resultSet = session.execute(bound);
            boolean exists = resultSet.one() != null;
            
            log.debug("Verificação de existência do usuário {}: {}", userId, exists);
            return exists;
        } catch (Exception e) {
            log.error("Erro ao verificar existência do usuário: " + e.getMessage());
            throw new RuntimeException("Falha ao verificar existência do usuário", e);
        }
    }

    public void deleteById(String userId) {
        try {
            String deleteQuery = "DELETE FROM users WHERE user_id = ?";
            PreparedStatement prepared = session.prepare(deleteQuery);
            BoundStatement bound = prepared.bind(userId);
            
            session.execute(bound);
            log.debug("Usuário deletado: {}", userId);
        } catch (Exception e) {
            log.error("Erro ao deletar usuário: " + e.getMessage());
            throw new RuntimeException("Falha ao deletar usuário", e);
        }
    }

    public void updateUser(User user) {
        try {
            String updateQuery = "UPDATE users SET name = ?, registered_at = ? WHERE user_id = ?";
            PreparedStatement prepared = session.prepare(updateQuery);
            BoundStatement bound = prepared.bind(
                    user.getName(),
                    user.getRegisteredAt(),
                    user.getUserId()
            );
            
            session.execute(bound);
            log.debug("Usuário atualizado: {}", user.getUserId());
        } catch (Exception e) {
            log.error("Erro ao atualizar usuário: " + e.getMessage());
            throw new RuntimeException("Falha ao atualizar usuário", e);
        }
    }

    public List<User> findByNameContaining(String namePattern) {
        List<User> users = new ArrayList<>();
        try {
            String selectQuery = "SELECT user_id, name, registered_at FROM users WHERE name LIKE ? ALLOW FILTERING";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind("%" + namePattern + "%");
            
            ResultSet resultSet = session.execute(bound);

            for (Row row : resultSet) {
                users.add(new User(
                        row.getString("user_id"),
                        row.getString("name"),
                        row.getInstant("registered_at")
                ));
            }
            log.debug("Encontrados {} usuários com nome contendo: {}", users.size(), namePattern);
        } catch (Exception e) {
            log.error("Erro ao buscar usuários por nome: " + e.getMessage());
            throw new RuntimeException("Falha ao buscar usuários por nome", e);
        }
        return users;
    }
}