package ifsc.edu.programacaodistribuidaeconcorrente;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class PresenceEventListener {
    
    private final PresenceCRDTService crdtService;
    
    public PresenceEventListener(PresenceCRDTService crdtService) {
        this.crdtService = crdtService;
    }
    
    @RabbitListener(queues = "#{anonymousQueue.name}")
    public void onPresenceEvent(PresenceEvent event) {
        if (event.type == PresenceEvent.Type.ADD) {
            crdtService.add(event.deviceId, event.tag);
        } else {
            crdtService.remove(event.deviceId, event.tag);
        }
    }
}
