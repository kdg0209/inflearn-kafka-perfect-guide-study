package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * kafka consumer graceful shutdown
 * kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t1 --partitions 3
 * kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t2 --partitions 3
 * kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t1
 * kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t2
 * kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group group-round-robin-topic
 */

public class ConsumerRoundRobin {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRoundRobin.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "group-round-robin-topic";
    private static final String TOPIC_A_NAME = "topic-p3-t1";
    private static final String TOPIC_B_NAME = "topic-p3-t2";

    public static void main(String[] args) {

        // kafka consumer settings
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        // kafka broker subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC_A_NAME, TOPIC_B_NAME));

        Thread mainThread = Thread.currentThread();

        // main 스레드 종료시 별도의 thread로 consumer wakeup 메서드 호출
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("wake up call");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                LOGGER.error("main thread dead");
            }
        }));

        // poll
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("topic: {}, record key: {}, partition: {}, record_offset: {}, value: {}", record.topic(), record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            LOGGER.error("wake up exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            consumer.close();
        }

    }
}
