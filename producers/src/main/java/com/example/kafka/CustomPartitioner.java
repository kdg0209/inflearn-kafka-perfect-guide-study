package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public static final Logger LOGGER = LoggerFactory.getLogger(CustomPartitioner.class.getName());

    private String specialKeyName;
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5);

        int result = 0;
        if (keyBytes == null) {
            result = stickyPartitionCache.partition(topic, cluster);
        } else if (key.toString().equals(specialKeyName)) {
            result = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            result = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        LOGGER.info("key: {} is sent to partition: {}", key, result);
        return result;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }
}
