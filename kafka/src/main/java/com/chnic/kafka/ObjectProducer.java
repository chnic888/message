package com.chnic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class ObjectProducer {

    private static final String TOPIC_NAME = "test.topic.2";

    public static void main(String[] args) {
        log.info("Producer Start...");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.100:9092,192.168.0.101:9092,192.168.0.102:9092");
        //0 1 all
        properties.put("acks", "0");
        properties.put("compression.type", "snappy");
        properties.put("retries", "3");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.chnic.kafka.UserSerializer");

        KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(properties);
        Random random = new Random();
        try {
            IntStream.range(0, 100000).forEach(index -> {
                User user = User.builder().id(index).name("test_name").gender(index % 2 == 0 ? "M" : "F").birthday(new Date()).build();
                ProducerRecord<String, User> producerRecord = new ProducerRecord<>(TOPIC_NAME, "key", user);
                kafkaProducer.send(producerRecord);
                log.info("Ready to send message key {}, value {}", "key", user.toString());

                try {
                    TimeUnit.SECONDS.sleep(random.nextInt(3));
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            });

        } finally {
            kafkaProducer.close();
        }

    }
}
