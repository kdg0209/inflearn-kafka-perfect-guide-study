package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * key 값을 사용하여 특정 파티션으로 message 전송
 * kafka-topics --bootstrap-server localhost:9092 --create --topic multipart-topic --partitions 3
 * kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic multipart-topic --property print.key=true --property print.value=true --property print.partition=true
 */
public class ProducerAsyncWithKey {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerAsyncWithKey.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "multipart-topic";

    public static void main(String[] args) throws InterruptedException {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. kafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {

            // 3. kafkaProducerRecord object create
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, String.valueOf(seq), "hello-world " + seq);
            LOGGER.info("seq : {}", seq);

            // 4. producer message send
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
        }

        Thread.sleep(1000);
    }
}
