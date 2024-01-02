package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
 * kafka-console-producer --bootstrap-server localhost:9092 --topic simple-topic
 */
public class SimpleConsumer {

    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "group_01";
    private static final String TOPIC_NAME = "simple-topic";

    public static void main(String[] args) {

        // kafka consumer settings
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);

        // kafka broker subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC_NAME));

        // poll
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("record key: {}, value: {}, partition: {}", record.key(), record.value(), record.partition());
            }
        }
    }
}
