package ifsc.edu.programacaodistribuidaeconcorrente.model;

import java.util.UUID;

public class PresenceEvent {
    public enum Type { ADD, REMOVE }
    public String deviceId;
    public UUID tag;
    public Type type;
}
