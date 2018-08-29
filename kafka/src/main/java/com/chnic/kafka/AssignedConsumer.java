package com.chnic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

@Slf4j
public class AssignedConsumer {

    private static final String TOPIC_NAME = "test.topic.1";

    public static void main(String[] args) {
        log.info("AssignedConsumer Start...");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.100:9092,192.168.0.101:9092,192.168.0.102:9092");
        properties.put("group.id", "Tester");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(TOPIC_NAME);

        List<TopicPartition> subscribedList = partitionInfoList.stream()
                .filter(partitionInfo -> partitionInfo.partition() == 0)
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(toList());
        kafkaConsumer.assign(subscribedList);
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000));
                consumerRecords.forEach(record -> {
                    log.info("partition: {}, offset: {}, key {}, value: {}", record.partition(), record.offset(), record.key(), record.value());
                });
            }
        } finally {
            kafkaConsumer.close();
            log.info("Consumer End...");
        }
    }
}
