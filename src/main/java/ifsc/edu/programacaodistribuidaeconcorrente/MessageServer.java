package ifsc.edu.programacaodistribuidaeconcorrente;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageServer {

    @Autowired
    private DatabaseConfig databaseConfig;
    @Autowired
    private QueueManagementConfig queueManagementConfig;

    private volatile boolean isRunning = false;

    @PostConstruct
    public void init() {
        log.info("MessageServer inicializado pelo Spring");
    }

    public void start() {
        try {
            log.info("Iniciando MessageServer...");

            databaseConfig.initCassandra();
            log.info("Database configurado com sucesso");

            queueManagementConfig.init();
            log.info("Queue Management configurado com sucesso");

            isRunning = true;
            log.info("MessageServer iniciado com sucesso!");

            while (isRunning) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (Exception e) {
            log.error("Erro ao iniciar servidor: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Falha ao iniciar MessageServer", e);
        }
    }

    public void shutdown() {
        log.info("Parando MessageServer...");
        isRunning = false;

        try {
            if (databaseConfig != null) {
                databaseConfig.close();
            }

            log.info("MessageServer parado com sucesso");

        } catch (Exception e) {
            log.error("Erro ao parar servidor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void cleanup() {
        log.info("Executando cleanup do MessageServer...");
        shutdown();
    }
}
