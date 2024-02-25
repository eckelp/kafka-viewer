package tech.geservices.kafkaadmin.gateways;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KafkaAdminGateway {
    Set<?> getTopics();

    List<Map<String, String>> getMessages(String topic);
}
