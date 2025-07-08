package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DevicePresencePoller {
    
    private final RabbitMqPresenceChecker presenceChecker;
    private final MessagingService messagingService;
    
    // Memory-based last-seen status map
    private final Map<String, Boolean> onlineStatus = new ConcurrentHashMap<>();
    
    public DevicePresencePoller(RabbitMqPresenceChecker presenceChecker, MessagingService messagingService) {
        this.presenceChecker = presenceChecker;
        this.messagingService = messagingService;
    }
    
    @Scheduled(fixedRate = 5000)
    public void checkPresence() {
        List<String> userIds = List.of("user1", "user2");
        List<String> deviceIds = List.of("deviceA", "deviceB");
        
        for (String userId : userIds) {
            for (String deviceId : deviceIds) {
                String key = userId + ":" + deviceId;
                boolean isOnline = presenceChecker.isDeviceOnline(userId, deviceId);
                boolean wasOnline = onlineStatus.getOrDefault(key, false);
                
                if (!wasOnline && isOnline) {
                    // Device just came online
                    messagingService.onDeviceReconnect(userId, deviceId);
                }
                
                onlineStatus.put(key, isOnline);
            }
        }
    }
}
