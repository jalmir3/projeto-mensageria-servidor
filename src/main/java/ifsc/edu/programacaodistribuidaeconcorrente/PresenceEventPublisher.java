package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import java.util.UUID;

@Service
public class PresenceEventPublisher {
    
    private final RabbitTemplate rabbitTemplate;
    
    public PresenceEventPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }
    
    public void publishAdd(String deviceId, UUID tag) {
        PresenceEvent event = new PresenceEvent();
        event.deviceId = deviceId;
        event.tag = tag;
        event.type = PresenceEvent.Type.ADD;
        rabbitTemplate.convertAndSend("presence-exchange", "", event);
    }
    
    public void publishRemove(String deviceId, UUID tag) {
        PresenceEvent event = new PresenceEvent();
        event.deviceId = deviceId;
        event.tag = tag;
        event.type = PresenceEvent.Type.REMOVE;
        rabbitTemplate.convertAndSend("presence-exchange", "", event);
    }
}