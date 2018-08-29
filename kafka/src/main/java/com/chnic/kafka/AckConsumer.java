package com.chnic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class AckConsumer {

    private static final String TOPIC_NAME = "test.topic.1";

    public static void main(String[] args) {
        log.info("Consumer Start...");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.100:9092,192.168.0.101:9092,192.168.0.102:9092");
        properties.put("group.id", "Tester");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));
                consumerRecords.forEach(record -> {
                    log.info("partition: {}, offset: {}, key {}, value: {}", record.partition(), record.offset(), record.key(), record.value());
                });

//                kafkaConsumer.commitSync();
//                log.info("commitSync...");

//                kafkaConsumer.commitAsync();
//                log.info("commitAsync...");

                kafkaConsumer.commitAsync(((offsets, exception) -> {
                    if (exception != null) {
                        log.error("commitAsync with callback, {}", exception.getMessage());
                    } else {
                        log.info("commitAsync with callback, {}", offsets);
                    }
                }));
            }
        } finally {
            kafkaConsumer.close();
            log.info("Consumer End...");
        }

    }
}
