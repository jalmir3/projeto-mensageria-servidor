package ifsc.edu.programacaodistribuidaeconcorrente;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        log.info("Iniciando aplicação Spring Boot...");

        try {
            // Inicializar o contexto do Spring
            ConfigurableApplicationContext context = SpringApplication.run(Main.class, args);

            // Obter o bean do MessageServer
            MessageServer server = context.getBean(MessageServer.class);

            // Configurar shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("\nEncerrando servidor...");
                server.shutdown();
                context.close();
                log.info("Aplicação encerrada com sucesso.");
            }));

            server.start();

        } catch (Exception e) {
            log.error("Erro ao iniciar aplicação: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}