package com.chnic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Producer {

    private static final String TOPIC_NAME = "test.topic.1";

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
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("partitioner.class", "com.chnic.kafka.producer.TestPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "key1", "value1");

        //Asynchronously send message without callback
//        kafkaProducer.send(producerRecord);

        //Asynchronously send message with callback
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (Optional.ofNullable(exception).isPresent()) {
                log.info("Exception: {}", exception.getMessage());
            } else {
                log.info("RecordMetadata: {}", metadata.toString());
            }
        });

        //Synchronously send message
//        try {
//            kafkaProducer.send(producerRecord).get();
//        } catch (InterruptedException | ExecutionException e) {
//            log.error(e.getMessage());
//        }

        kafkaProducer.close();
        log.info("Producer End...");
    }
}
