package tech.geservices.kafkaadmin.usecases;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tech.geservices.kafkaadmin.gateways.KafkaAdminGateway;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ListMessages {

    private final KafkaAdminGateway kafkaAdminGateway;

    public List<Map<String, String>> execute(String topic) {
        return kafkaAdminGateway.getMessages(topic);
    }

}
