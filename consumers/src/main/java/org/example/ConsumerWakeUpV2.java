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
public class ConsumerWakeUpV2 {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWakeUpV2.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "group-02";
    private static final String TOPIC_NAME = "pizza-topic";

    public static void main(String[] args) {

        // kafka consumer settings
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

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
        long count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                LOGGER.info("############## count: {}, ConsumerRecords count: {}", count++, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("record key: {}, value: {}, partition: {}, record_offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }

                try {
                    LOGGER.info("main thread is sleeping {} ms during while loop ", (count * 10000));
                    Thread.sleep((count * 10000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
