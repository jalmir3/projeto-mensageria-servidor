package ifsc.edu.programacaodistribuidaeconcorrente;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableScheduling
public class Main {
    public static void main(String[] args) {
        log.info("Iniciando aplicação Spring Boot...");
        SpringApplication.run(Main.class, args);
    }
}