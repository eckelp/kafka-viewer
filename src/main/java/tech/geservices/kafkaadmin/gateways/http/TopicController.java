package tech.geservices.kafkaadmin.gateways.http;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.geservices.kafkaadmin.usecases.ListMessages;
import tech.geservices.kafkaadmin.usecases.ListTopics;

@RestController
@RequestMapping("/topics")
@RequiredArgsConstructor
public class TopicController {

    private final ListTopics listTopics;
    private final ListMessages listMessages;


    @GetMapping
    public ResponseEntity<?> getAll() {
        return ResponseEntity.ok(listTopics.execute());
    }

    @GetMapping("/{topic}")
    public ResponseEntity<?> getMessages(@PathVariable String topic) {
        return ResponseEntity.ok(listMessages.execute(topic));
    }

}
