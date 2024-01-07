package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

/**
 * kafka-topics --bootstrap-server localhost:9092 --create --topic file-topic
 */
public class BaseConsumerMain {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "file-group";
    private static final String TOPIC_NAME = "file-topic";
    private static final String ENABLE_AUTO_COMMIT = "false";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(props, List.of(TOPIC_NAME));
        baseConsumer.initConsumer();

        String commitMode = "async";
        long durationMillis = 100L;
        baseConsumer.pollConsumes(durationMillis, commitMode);
        baseConsumer.closeConsumer();
    }
}
