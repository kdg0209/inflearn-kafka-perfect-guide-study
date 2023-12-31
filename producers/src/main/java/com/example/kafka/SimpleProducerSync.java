package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {

    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "simple-topic";

    public static void main(String[] args) {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. kafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 3. kafkaProducerRecord object create
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "hello-world sync 4");

        // 4. producer message send
        try {
            // get method로 인한 blocking
            RecordMetadata metadata = kafkaProducer.send(record).get();
            LOGGER.info("############################################################");
            LOGGER.info("record metadata received ");
            LOGGER.info("partition : {}", metadata.partition());
            LOGGER.info("offset : {}", metadata.offset());
            LOGGER.info("has offset : {}", metadata.hasOffset());
            LOGGER.info("timestamp : {}", metadata.timestamp());
            LOGGER.info("############################################################");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
