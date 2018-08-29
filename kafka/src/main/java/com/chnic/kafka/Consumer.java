package com.chnic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class Consumer {

    private static final String TOPIC_NAME = "test.topic.1";

    public static void main(String[] args) {
        log.info("Consumer Start...");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.100:9092,192.168.0.101:9092,192.168.0.102:9092");
        properties.put("group.id", "Tester");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown.....");
            kafkaConsumer.wakeup();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }));

        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        //subscribe topic by regex
//        kafkaConsumer.subscribe(Pattern.compile("test.*"));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(record -> {
                    log.info("partition: {}, offset: {}, key {}, value: {}", record.partition(), record.offset(), record.key(), record.value());
                });
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } finally {
            kafkaConsumer.close();
            log.info("Consumer End...");
        }

    }
}
