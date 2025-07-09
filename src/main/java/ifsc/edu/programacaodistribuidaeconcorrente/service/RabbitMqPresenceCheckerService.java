package ifsc.edu.programacaodistribuidaeconcorrente.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Service
public class RabbitMqPresenceCheckerService {
    private final RestTemplate restTemplate;
    private final HttpHeaders headers;
    private final String baseUrl = "http://localhost:15672/api/queues/%2F/";

    public RabbitMqPresenceCheckerService() {
        this.restTemplate = new RestTemplate();
        String auth = "guest:guest";
        String encoded = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        this.headers = new HttpHeaders();
        headers.set("Authorization", "Basic " + encoded);
    }

    public boolean isDeviceOnline(String userId, String deviceId) {
        String queueName = "messages.user." + userId + ".device." + deviceId;
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        try {
            ResponseEntity<JsonNode> res = restTemplate.exchange(baseUrl + queueName, HttpMethod.GET, entity, JsonNode.class);
            return res.getBody().get("consumers").asInt() > 0;
        } catch (HttpClientErrorException.NotFound e) {
            return false;
        }
    }
}

