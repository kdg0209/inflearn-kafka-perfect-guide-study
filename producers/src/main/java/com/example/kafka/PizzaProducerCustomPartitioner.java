package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic-partitioner --partitions 5 토픽 생성
 * kafka-console-consumer --bootstrap-server localhost:9092 --group group_01 --topic pizza-topic-partitioner --property print.key=true --property print.value=true --property print.partition=true 콘솔 컨슈머 5개 생성
 */
public class PizzaProducerCustomPartitioner {

    public static final Logger LOGGER = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    private static final String SPECIAL_SHOP_KEY = "P001";
    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "pizza-topic-partitioner";

    public static void main(String[] args) throws InterruptedException {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // custom partition 적용
        properties.setProperty("custom.specialKey", SPECIAL_SHOP_KEY); // custom partitioner에서 사용할 키
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");

        // 2. kafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 3. message send
        var number = -1; // -1 이라면 while문에서 무한루프
        var interIntervalMillis = 100;
        var intervalMillis = 1000;
        var intervalCount = 100;
        var isSync = true;
        sendPizzaMessage(kafkaProducer, number, interIntervalMillis, intervalMillis, intervalCount, isSync);

        Thread.sleep(1000);
    }

    private static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer, int number, int interIntervalMillis, int intervalMillis, int intervalCount, boolean isSync) {
        var pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        var seed = 2022L;
        var random = new Random(seed);
        var faker = Faker.instance(random);

        while (number != iterSeq) {
            iterSeq++;

            // kafkaProducerRecord object create
            var message = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message.get("key"), message.get("message"));

            // sned
            if (isSync) {
                sendMessageViaSync(kafkaProducer, record, message);
            } else {
                sendMessageViaAsync(kafkaProducer, record, message);
            }

            // Thread sleep을 이용한 텀 주기
            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    LOGGER.info("###### intervalCount: {}, intervalMillis: {} ######", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    LOGGER.info("###### interIntervalMillis: {} ######", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void sendMessageViaAsync(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record, HashMap<String, String> map) {
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                LOGGER.info("############################################################");
                LOGGER.info("async message : {}", map.get("key"));
                LOGGER.info("partition : {}", metadata.partition());
                LOGGER.info("offset : {}", metadata.offset());
                LOGGER.info("has offset : {}", metadata.hasOffset());
                LOGGER.info("timestamp : {}", metadata.timestamp());
                LOGGER.info("############################################################");
            }
        });
    }

    private static void sendMessageViaSync(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record, HashMap<String, String> map) {
        try {
            RecordMetadata metadata = kafkaProducer.send(record).get();
            LOGGER.info("############################################################");
            LOGGER.info("sync message : {}", map.get("key"));
            LOGGER.info("partition : {}", metadata.partition());
            LOGGER.info("offset : {}", metadata.offset());
            LOGGER.info("has offset : {}", metadata.hasOffset());
            LOGGER.info("timestamp : {}", metadata.timestamp());
            LOGGER.info("############################################################");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
