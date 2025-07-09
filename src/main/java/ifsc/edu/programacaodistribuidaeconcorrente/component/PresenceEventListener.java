package ifsc.edu.programacaodistribuidaeconcorrente.component;

import ifsc.edu.programacaodistribuidaeconcorrente.model.PresenceEvent;
import ifsc.edu.programacaodistribuidaeconcorrente.service.PresenceCRDTService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class PresenceEventListener {
    
    private final PresenceCRDTService crdtService;
    
    public PresenceEventListener(PresenceCRDTService crdtService) {
        this.crdtService = crdtService;
    }
    
    @RabbitListener(queues = "#{anonymousQueue.name}")
    public void onPresenceEventAnonymous(PresenceEvent event) {
        processPresenceEvent(event);
    }
    
    private void processPresenceEvent(PresenceEvent event) {
        if (event.type == PresenceEvent.Type.ADD) {
            crdtService.add(event.deviceId, event.tag);
        } else {
            crdtService.remove(event.deviceId, event.tag);
        }
    }
}