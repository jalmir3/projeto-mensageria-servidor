package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.stereotype.Service;
import java.time.Instant;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final RabbitMqUserSetupService rabbitMqUserSetupService;

    public UserService(UserRepository userRepository,
                       RabbitMqUserSetupService rabbitMqUserSetupService) {
        this.userRepository = userRepository;
        this.rabbitMqUserSetupService = rabbitMqUserSetupService;
    }

    public User registerUser(String userId, String name) {
        User user = new User(userId, name, Instant.now());
        userRepository.save(user);

        // Create RabbitMQ queue and binding for this user
        rabbitMqUserSetupService.createUserQueue(userId);

        return user;
    }
}
