package ifsc.edu.programacaodistribuidaeconcorrente;

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
    private String receiver;
    private String content;
    private long timestamp;
    private String status;
    private Boolean delivered;
}