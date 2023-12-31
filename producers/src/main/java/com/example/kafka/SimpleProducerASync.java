package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerASync {

    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "simple-topic";

    public static void main(String[] args) throws InterruptedException {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. kafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 3. kafkaProducerRecord object create
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "hello-world async 5");


        // 4. producer message send
        // callBack을 사용한 async
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                LOGGER.info("############################################################");
                LOGGER.info("record metadata received ");
                LOGGER.info("partition : {}", metadata.partition());
                LOGGER.info("offset : {}", metadata.offset());
                LOGGER.info("has offset : {}", metadata.hasOffset());
                LOGGER.info("timestamp : {}", metadata.timestamp());
                LOGGER.info("############################################################");
            }
        });

        Thread.sleep(1000);
    }
}
