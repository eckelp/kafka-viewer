package tech.geservices.kafkaadmin.gateways.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tech.geservices.kafkaadmin.gateways.KafkaAdminGateway;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaAdminGatewayImpl implements KafkaAdminGateway {

    @Value("${kafka.clusters:localhost:9092}")
    private String clusters;

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final Map<TopicPartition, Long> lastReadOffsets = new ConcurrentHashMap<>();

    @Override
    public Set<?> getTopics() {
        try {
            AdminClient adminClient = AdminClient.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusters
            ));

            ListTopicsResult topicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(true));
            KafkaFuture<Set<String>> topics = topicsResult.names();

            adminClient.close();

            return topics.get();
        } catch (Exception e) {
            log.error("Fail to get topics - {}", e.getMessage(), e);
            return Set.of();
        }
    }

//    public ConsumerRecords<String, String> getMessages(String topic) {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusters);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "meu-group-id");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//            consumer.subscribe(Collections.singletonList(topic));
//
//            consumer.assignment().forEach(partition -> {
//                consumer.seek(partition, lastReadOffsets.getOrDefault(partition, 0L));
//            });
//
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
//
//            records.forEach(record -> {
//                log.info("Record - Key: {} | Message: {}", record.key(), record.value());
//                lastReadOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
//            });
//
//            return records;
//        }catch (Exception e ){
//            e.printStackTrace();
//            throw e;
//        }
//    }
    private static final int POLL_TIMEOUT_MS = 200;

    @Override
    public synchronized List<Map<String, String>> getMessages(String topic) {
        try {
            int count = 10;
            final List<TopicPartition> partitions = determinePartitionsForTopic(topic);
            kafkaConsumer.assign(partitions);
            final var latestOffsets = kafkaConsumer.endOffsets(partitions);

            for (var partition : partitions) {
                final var latestOffset = Math.max(0, latestOffsets.get(partition) - 1);
                kafkaConsumer.seek(partition, Math.max(0, latestOffset - count));
            }

            final var totalCount = count * partitions.size();
            final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> rawRecords
                    = partitions.stream().collect(Collectors.toMap(p -> p, p -> new ArrayList<>(count)));

            var moreRecords = true;
            while (rawRecords.size() < totalCount && moreRecords) {
                final var polled = kafkaConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                moreRecords = false;
                for (var partition : polled.partitions()) {
                    var records = polled.records(partition);
                    if (!records.isEmpty()) {
                        rawRecords.get(partition).addAll(records);
                        moreRecords = records.get(records.size() - 1).offset() < latestOffsets.get(partition) - 1;
                    }
                }
            }

            return rawRecords
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .map(rec -> createConsumerRecord(rec))
                    .toList();
        }catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static Map<String, String> createConsumerRecord(ConsumerRecord<byte[], byte[]> rec) {
        final var key = ByteUtils.readString(ByteBuffer.wrap(rec.key()));
        final var value = ByteUtils.readString(ByteBuffer.wrap(rec.value()));

        Map<String, String> map = new HashMap<>();
        map.put(key, value);

        return map;

//        return new ConsumerRecord<>(rec.topic(), rec.partition(), rec.offset(), rec.timestamp(), rec.timestampType(),
//                rec.serializedKeySize(), rec.serializedValueSize(), ByteUtils.readString(ByteBuffer.wrap(rec.key())),
//                ByteUtils.readString(ByteBuffer.wrap(rec.value())), rec.headers(),
//                rec.leaderEpoch());
    }

    private List<TopicPartition> determinePartitionsForTopic(String topic) {
        return kafkaConsumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .toList();
    }

}
