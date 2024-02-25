package tech.geservices.kafkaadmin.usecases;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tech.geservices.kafkaadmin.gateways.KafkaAdminGateway;

import java.util.Set;

@Component
@RequiredArgsConstructor
public class ListTopics {

    private final KafkaAdminGateway kafkaAdminGateway;

    public Set<?> execute() {
        return kafkaAdminGateway.getTopics();
    }

}
