package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * kafka consumer graceful shutdown
 * kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
 * kafka-console-producer --bootstrap-server localhost:9092 --topic simple-topic
 */
public class ConsumerWakeUp {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWakeUp.class.getName());

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
                    LOGGER.info("record key: {}, value: {}, partition: {}, record_offset: {}", record.key(), record.value(), record.partition(), record.offset());
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
