package ifsc.edu.programacaodistribuidaeconcorrente.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private UUID id;
    private String sender;
    private String recipient;
    private String content;
    private long timestamp;
}