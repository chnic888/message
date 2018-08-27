package com.chnic.kafka;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.Map;

public class LogPartitioner implements Partitioner {

    private Map<String, Integer> levelMap = Maps.newHashMap();

    public LogPartitioner() {
        levelMap.put("INFO", 0);
        levelMap.put("WARNING", 1);
        levelMap.put("ERROR", 2);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!levelMap.containsKey(key.toString())) {
            throw new InvalidRecordException("Invalid message key: " + key);
        }

        if (cluster.partitionCountForTopic(topic) != levelMap.size()) {
            throw new InvalidTopicException("Invalid partition count");
        }

        return levelMap.get(key.toString());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
