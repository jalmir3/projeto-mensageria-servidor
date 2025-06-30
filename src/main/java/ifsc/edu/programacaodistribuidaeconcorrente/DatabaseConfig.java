package ifsc.edu.programacaodistribuidaeconcorrente;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
@Slf4j
@Component
public class DatabaseConfig {

    @Value("${cassandra.contact-points}")
    private String contactPoints;

    @Value("${cassandra.port}")
    private int port;

    @Getter
    @Value("${cassandra.keyspace}")
    private String keyspace;

    @Value("${cassandra.datacenter}")
    private String datacenter;

    @Value("${cassandra.username:#{null}}")
    private String username;

    @Value("${cassandra.password:#{null}}")
    private String password;

    @Value("${cassandra.connection.timeout}")
    private int connectionTimeout;

    @Value("${cassandra.request.timeout}")
    private int requestTimeout;

    @Value("${cassandra.pool.local.core-connections}")
    private int localCoreConnections;

    @Value("${cassandra.pool.local.max-connections}")
    private int localMaxConnections;

    @Getter
    private CqlSession session;

    @Bean
    public CqlSession session() {
        try {
            return CqlSession.builder()
                    .withKeyspace("message_system")
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                    .withLocalDatacenter("datacenter1")
                    .build();
        } catch (Exception e) {
            log.error("Erro ao conectar ao Cassandra", e);
            throw new RuntimeException("Falha na conexão com Cassandra", e);
        }
    }

    @PostConstruct
    public void postConstruct() {
        log.info("DatabaseConfig inicializado pelo Spring");
        log.info("Contact Points: " + contactPoints);
        log.info("Keyspace: " + keyspace);
        log.info("Datacenter: " + datacenter);
    }

    public void initCassandra() {
        try {
            log.info("=== Iniciando conexão com Cassandra ===");
            log.info("Contact Points: " + contactPoints);
            log.info("Port: " + port);
            log.info("Keyspace: " + keyspace);
            log.info("Datacenter: " + datacenter);

            // Preparar lista de contact points
            List<String> hosts = Arrays.asList(contactPoints.split(","));

            // Criar configuração programática
            ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(requestTimeout))
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(connectionTimeout))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, localCoreConnections)
                    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, localMaxConnections);

            DriverConfigLoader configLoader = configLoaderBuilder.build();

            // Primeiro, conectar sem keyspace para criar o keyspace se necessário
            CqlSessionBuilder builderWithoutKeyspace = CqlSession.builder();

            // Adicionar contact points
            for (String host : hosts) {
                builderWithoutKeyspace.addContactPoint(new InetSocketAddress(host.trim(), port));
            }

            // Configurar datacenter
            builderWithoutKeyspace.withLocalDatacenter(datacenter);

            // Aplicar configuração personalizada
            builderWithoutKeyspace.withConfigLoader(configLoader);

            // Configurar autenticação se fornecida
            if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
                log.info("Configurando autenticação para usuário: " + username);
                builderWithoutKeyspace.withAuthCredentials(username, password);
            }

            // Criar sessão temporária sem keyspace
            CqlSession tempSession = builderWithoutKeyspace.build();

            // Criar keyspace se não existir
            log.info("Verificando se keyspace existe: " + keyspace);
            String createKeyspaceQuery = String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 1}",
                    keyspace
            );
            tempSession.execute(createKeyspaceQuery);
            log.info("✅ Keyspace verificado/criado: " + keyspace);

            // Fechar sessão temporária
            tempSession.close();

            // Agora conectar com o keyspace
            CqlSessionBuilder builder = CqlSession.builder();

            // Adicionar contact points
            for (String host : hosts) {
                builder.addContactPoint(new InetSocketAddress(host.trim(), port));
            }

            // Configurar datacenter
            builder.withLocalDatacenter(datacenter);

            // Configurar keyspace
            builder.withKeyspace(keyspace);

            // Aplicar configuração personalizada
            builder.withConfigLoader(configLoader);

            // Configurar autenticação se fornecida
            if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
                builder.withAuthCredentials(username, password);
            }

            // Criar a sessão final
            session = builder.build();

            // Testar conexão
            String query = "SELECT release_version FROM system.local";
            var result = session.execute(query);
            var version = result.one().getString("release_version");

            log.info("✅ Cassandra conectado com sucesso!");
            log.info("Versão do Cassandra: " + version);
            log.info("Keyspace ativo: " + session.getKeyspace().map(CqlIdentifier::toString).orElse("N/A"));

        } catch (Exception e) {
            log.error("❌ Erro ao conectar com Cassandra: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Falha na conexão com Cassandra", e);
        }
    }

    @PreDestroy
    public void close() {
        try {
            if (session != null && !session.isClosed()) {
                log.info("Fechando conexão com Cassandra...");
                session.close();
                log.info("✅ Conexão Cassandra fechada com sucesso");
            }
        } catch (Exception e) {
            log.error("❌ Erro ao fechar conexão Cassandra: " + e.getMessage());
            e.printStackTrace();
        }
    }
}