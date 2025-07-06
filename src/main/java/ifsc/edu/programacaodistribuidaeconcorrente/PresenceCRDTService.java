package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.stereotype.Service;
import java.util.Set;
import java.util.UUID;

@Service
public class PresenceCRDTService {
    private final ORSet orSet = new ORSet();
    
    public synchronized void add(String deviceId, UUID tag) {
        orSet.add(deviceId, tag);
    }
    
    public synchronized void remove(String deviceId, UUID tag) {
        orSet.remove(deviceId, tag);
    }
    
    public boolean isOnline(String deviceId) {
        return orSet.contains(deviceId);
    }
    
    public Set<String> getOnlineDevices() {
        return orSet.getDeviceIds();
    }
}
