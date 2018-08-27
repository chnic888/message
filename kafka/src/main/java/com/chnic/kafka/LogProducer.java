package com.chnic.kafka;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LogProducer {

    private static final String TOPIC_NAME = "test.topic.1";

    public static void main(String[] args) throws InterruptedException {
        log.info("LogProducer Start...");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.100:9092,192.168.0.101:9092,192.168.0.102:9092");
        //0 1 all
        properties.put("acks", "all");
        properties.put("compression.type", "snappy");
        properties.put("retries", "3");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.chnic.kafka.LogPartitioner");

        Map<Integer, String> levelMap = Maps.newHashMap();
        levelMap.put(0, "INFO");
        levelMap.put(1, "WARNING");
        levelMap.put(2, "ERROR");
        levelMap.put(3, "DEBUG");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        try {
            while (true) {
                String level = levelMap.get(new Random().nextInt(100) % 4);
                String value = new Date().toString();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, level, String.format("[%s]: message %s", level, value));
                log.info("Ready to send message key {}, value {}", level, value);

                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (Optional.ofNullable(exception).isPresent()) {
                        log.info("Exception: {}", exception.getMessage());
                    } else {
                        log.info("RecordMetadata: {}", metadata.toString());
                    }
                });

                TimeUnit.SECONDS.sleep(5);
            }
        } finally {
            kafkaProducer.close();
            log.info("LogProducer End...");
        }


    }
}
