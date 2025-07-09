package ifsc.edu.programacaodistribuidaeconcorrente.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OfflineMessage {
    
    public static class Key {
        private String userId;
        private String deviceId;
        private Instant createdAt;
        
        public Key() {}
        
        public Key(String userId, String deviceId, Instant createdAt) {
            this.userId = userId;
            this.deviceId = deviceId;
            this.createdAt = createdAt;
        }
        
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        
        public String getDeviceId() { return deviceId; }
        public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
        
        public Instant getCreatedAt() { return createdAt; }
        public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    }

    private Key key;
    private UUID messageId;
    private String senderId;
    private String content;
}

