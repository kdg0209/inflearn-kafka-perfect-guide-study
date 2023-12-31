package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * integer 타입의 key 값을 사용하여 특정 파티션으로 message 전송
 * kafka-console-consumer는 기본적으로 String 타입만 받을 수 있기 때문에 --key-deserializer 옵션 추가
 * kafka-topics --bootstrap-server localhost:9092 --create --topic multipart-topic --partitions 3
 * kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic multipart-topic --property print.key=true --property print.value=true --property partition=true --key-deserializer "org.apache.kafka.common.serialization.IntegerDeserializer"
 */
public class ProducerAsyncCustomCallback {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "multipart-topic";

    public static void main(String[] args) throws InterruptedException {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. kafkaProducer object create
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {

            // 3. kafkaProducerRecord object create
            ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC_NAME, seq, "hello-world " + seq);

            // 4. producer message send
            kafkaProducer.send(record, new CustomCallback(seq));
        }

        Thread.sleep(1000);
    }
}
