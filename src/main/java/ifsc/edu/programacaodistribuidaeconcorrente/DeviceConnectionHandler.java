package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DeviceConnectionHandler {
    private final PresenceEventPublisher publisher;
    private final Map<String, UUID> deviceTags = new ConcurrentHashMap<>();
    
    public DeviceConnectionHandler(PresenceEventPublisher publisher) {
        this.publisher = publisher;
    }
    
    public void onDeviceConnected(String deviceId) {
        UUID tag = UUID.randomUUID();
        deviceTags.put(deviceId, tag);
        publisher.publishAdd(deviceId, tag);
    }
    
    public void onDeviceDisconnected(String deviceId, UUID tag) {
        deviceTags.remove(deviceId);
        publisher.publishRemove(deviceId, tag);
    }
    
    public void onDeviceDisconnected(String deviceId) {
        UUID tag = deviceTags.remove(deviceId);
        if (tag != null) {
            publisher.publishRemove(deviceId, tag);
        }
    }
    
    public UUID getDeviceTag(String deviceId) {
        return deviceTags.get(deviceId);
    }
    
    public boolean isDeviceTracked(String deviceId) {
        return deviceTags.containsKey(deviceId);
    }
}
