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
    private final DeviceConnectionHandler connectionHandler;
    
    // Memory-based last-seen status map
    private final Map<String, Boolean> onlineStatus = new ConcurrentHashMap<>();
    
    public DevicePresencePoller(RabbitMqPresenceChecker presenceChecker, 
                               MessagingService messagingService,
                               DeviceConnectionHandler connectionHandler) {
        this.presenceChecker = presenceChecker;
        this.messagingService = messagingService;
        this.connectionHandler = connectionHandler;
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
                    connectionHandler.onDeviceConnected(deviceId);
                    messagingService.onDeviceReconnect(userId, deviceId);
                } else if (wasOnline && !isOnline) {
                    // Device went offline
                    connectionHandler.onDeviceDisconnected(deviceId);
                }
                
                onlineStatus.put(key, isOnline);
            }
        }
    }
}