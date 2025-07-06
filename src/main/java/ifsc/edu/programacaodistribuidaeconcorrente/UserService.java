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

    public void registerUser(String userId, String name, String deviceId) {
        User user = new User(userId, name, Instant.now());
        userRepository.save(user);

        rabbitMqUserSetupService.createDeviceQueue(userId, deviceId);
    }
}
