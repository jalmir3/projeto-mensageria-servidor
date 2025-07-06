package ifsc.edu.programacaodistribuidaeconcorrente;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessagingService {
    private final RabbitTemplate rabbitTemplate;
    private final MessageRepository messageRepository;
    private final DeviceRepository deviceRepository;
    private final OfflineMessageRepository offlineRepo;
    private final PresenceCRDTService presenceService;

    public MessagingService(RabbitTemplate rabbitTemplate,
                            MessageRepository messageRepository,
                            OfflineMessageRepository offlineRepo,
                            PresenceCRDTService presenceService,
                            DeviceRepository deviceRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.messageRepository = messageRepository;
        this.offlineRepo = offlineRepo;
        this.presenceService = presenceService;
        this.deviceRepository = deviceRepository;
    }

    public UUID sendMessage(String senderId, String receiverId, String content) {
        UUID messageId = UUID.randomUUID();
        Message message = new Message(
            messageId,
            senderId,
            receiverId,
            content,
            System.currentTimeMillis(),
            "sent",
            false
        );
        messageRepository.saveMessage(message);

        // Use the new method that leverages PresenceCRDTService
        MessagePayload payload = new MessagePayload(message);
        sendMessageToUserDevices(receiverId, payload);
        
        return messageId;
    }

    public void sendMessageToUserDevices(String userId, MessagePayload payload) {
        List<Device> devices = deviceRepository.findByKeyUserId(userId);
        for (Device device : devices) {
            String deviceId = device.getKey().getDeviceId();
            if (presenceService.isOnline(deviceId)) {
                // Send via WebSocket/RabbitMQ
                deliverToDevice(deviceId, payload);
            } else {
                // Queue for later delivery
                storeForOfflineDelivery(deviceId, payload);
            }
        }
    }

    private void deliverToDevice(String deviceId, MessagePayload payload) {
        String routingKey = "user." + payload.getMessage().getReceiver() + ".device." + deviceId;
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, routingKey, payload.getMessage());
    }

    private void storeForOfflineDelivery(String deviceId, MessagePayload payload) {
        Message message = payload.getMessage();
        OfflineMessage offline = new OfflineMessage();
        offline.setKey(new OfflineMessage.Key(message.getReceiver(), deviceId, Instant.now()));
        offline.setMessageId(message.getId());
        offline.setSenderId(message.getSender());
        offline.setContent(message.getContent());
        offlineRepo.saveOfflineMessage(offline);
    }

    public void onDeviceReconnect(String userId, String deviceId) {
        List<OfflineMessage> pending = offlineRepo.findByKeyUserIdAndKeyDeviceId(userId, deviceId);
        for (OfflineMessage msg : pending) {
            String routingKey = "user." + userId + ".device." + deviceId;
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, routingKey, msg);
            offlineRepo.deleteOfflineMessage(msg.getKey());
        }
    }

    public void markDelivered(UUID messageId) {
        Message message = messageRepository.findById(messageId);
        if (message != null){
            message.setDelivered(true);
            messageRepository.saveMessage(message);
        }
    }

    // Helper class for message payload
    public static class MessagePayload {
        private final Message message;

        public MessagePayload(Message message) {
            this.message = message;
        }

        public Message getMessage() {
            return message;
        }
    }
}
