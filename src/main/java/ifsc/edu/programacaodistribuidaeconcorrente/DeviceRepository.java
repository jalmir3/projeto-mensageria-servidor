package ifsc.edu.programacaodistribuidaeconcorrente;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class DeviceRepository {

    private final CqlSession session;

    @Autowired
    public DeviceRepository(CqlSession session) {
        this.session = session;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            // Verifica se a tabela já existe
            boolean tableExists = session.execute(
                    "SELECT table_name FROM system_schema.tables " +
                            "WHERE keyspace_name = 'message_system' AND table_name = 'devices'"
            ).one() != null;

            if (!tableExists) {
                String createTableQuery = "CREATE TABLE devices (" +
                        "user_id TEXT, " +
                        "device_id TEXT, " +
                        "registered_at TIMESTAMP, " +
                        "PRIMARY KEY (user_id, device_id))";

                session.execute(createTableQuery);
                log.info("✅ Tabela 'devices' criada no Cassandra");
            } else {
                // Verifica se a estrutura está correta
                verifyTableStructure();
                log.info("✅ Tabela 'devices' já existe");
            }
        } catch (Exception e) {
            log.error("❌ Erro ao criar/verificar tabela no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao criar tabela", e);
        }
    }

    private void verifyTableStructure() {
        try {
            // Tenta uma operação simples para verificar a estrutura
            session.execute("SELECT user_id, device_id, registered_at FROM devices LIMIT 1");
        } catch (Exception e) {
            log.error("Estrutura da tabela incorreta. Recriando tabela...");
            session.execute("DROP TABLE IF EXISTS devices");
            createTableIfNotExists();
        }
    }

    public void saveDevice(Device device) {
        try {
            if (device.getRegisteredAt() == null) {
                device.setRegisteredAt(Instant.now());
            }

            String insertQuery = "INSERT INTO devices (user_id, device_id, registered_at) " +
                    "VALUES (?, ?, ?)";

            PreparedStatement prepared = session.prepare(insertQuery);
            BoundStatement bound = prepared.bind(
                    device.getKey().getUserId(),
                    device.getKey().getDeviceId(),
                    device.getRegisteredAt()
            );

            session.execute(bound);
            log.debug("Device salvo no Cassandra: userId={}, deviceId={}", 
                    device.getKey().getUserId(), device.getKey().getDeviceId());
        } catch (Exception e) {
            log.error("Erro ao salvar device no Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao salvar device", e);
        }
    }

    public Device findByKey(Device.Key key) {
        try {
            String selectQuery = "SELECT user_id, device_id, registered_at FROM devices WHERE user_id = ? AND device_id = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(key.getUserId(), key.getDeviceId());
            
            ResultSet resultSet = session.execute(bound);
            Row row = resultSet.one();
            
            if (row == null) {
                log.debug("Device não encontrado com userId: {}, deviceId: {}", key.getUserId(), key.getDeviceId());
                return null;
            }
            
            Device.Key deviceKey = new Device.Key();
            deviceKey.setUserId(row.getString("user_id"));
            deviceKey.setDeviceId(row.getString("device_id"));
            
            Device device = new Device(
                    deviceKey,
                    row.getInstant("registered_at")
            );
            
            log.debug("Device encontrado: userId={}, deviceId={}", key.getUserId(), key.getDeviceId());
            return device;
            
        } catch (Exception e) {
            log.error("Erro ao buscar device por chave: " + e.getMessage());
            throw new RuntimeException("Falha ao buscar device por chave", e);
        }
    }

    public List<Device> findByKeyUserId(String userId) {
        List<Device> devices = new ArrayList<>();
        try {
            String selectQuery = "SELECT user_id, device_id, registered_at FROM devices WHERE user_id = ?";
            PreparedStatement prepared = session.prepare(selectQuery);
            BoundStatement bound = prepared.bind(userId);
            
            ResultSet resultSet = session.execute(bound);

            for (Row row : resultSet) {
                Device.Key deviceKey = new Device.Key();
                deviceKey.setUserId(row.getString("user_id"));
                deviceKey.setDeviceId(row.getString("device_id"));
                
                devices.add(new Device(
                        deviceKey,
                        row.getInstant("registered_at")
                ));
            }
            log.debug("Recuperados {} devices para o usuário: {}", devices.size(), userId);
        } catch (Exception e) {
            log.error("Erro ao recuperar devices por usuário: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar devices por usuário", e);
        }
        return devices;
    }

    public List<Device> getAllDevices() {
        List<Device> devices = new ArrayList<>();
        try {
            ResultSet resultSet = session.execute("SELECT user_id, device_id, registered_at FROM devices");

            for (Row row : resultSet) {
                Device.Key deviceKey = new Device.Key();
                deviceKey.setUserId(row.getString("user_id"));
                deviceKey.setDeviceId(row.getString("device_id"));
                
                devices.add(new Device(
                        deviceKey,
                        row.getInstant("registered_at")
                ));
            }
            log.debug("Recuperados {} devices do Cassandra", devices.size());
        } catch (Exception e) {
            log.error("Erro ao recuperar devices do Cassandra: " + e.getMessage());
            throw new RuntimeException("Falha ao recuperar devices", e);
        }
        return devices;
    }

    public void deleteDevice(Device.Key key) {
        try {
            String deleteQuery = "DELETE FROM devices WHERE user_id = ? AND device_id = ?";
            PreparedStatement prepared = session.prepare(deleteQuery);
            BoundStatement bound = prepared.bind(key.getUserId(), key.getDeviceId());
            
            session.execute(bound);
            log.debug("Device deletado: userId={}, deviceId={}", key.getUserId(), key.getDeviceId());
        } catch (Exception e) {
            log.error("Erro ao deletar device: " + e.getMessage());
            throw new RuntimeException("Falha ao deletar device", e);
        }
    }
}