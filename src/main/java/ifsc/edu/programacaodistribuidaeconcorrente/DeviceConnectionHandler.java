package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class DeviceConnectionHandler {
    private final PresenceEventPublisher publisher;
    
    public DeviceConnectionHandler(PresenceEventPublisher publisher) {
        this.publisher = publisher;
    }
    
    public void onDeviceConnected(String deviceId) {
        UUID tag = UUID.randomUUID(); // Unique session ID
        publisher.publishAdd(deviceId, tag);
        // Optionally: store mapping deviceId <-> tag for removal
    }
    
    public void onDeviceDisconnected(String deviceId, UUID tag) {
        publisher.publishRemove(deviceId, tag);
    }
}
