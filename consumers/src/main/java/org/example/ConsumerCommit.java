package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka consumer graceful shutdown
 * kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic
 * kafka-console-producer --bootstrap-server localhost:9092 --topic pizza-topic
 */
public class ConsumerCommit {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerCommit.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "group-03";
    private static final String TOPIC_NAME = "pizza-topic";

    public static void main(String[] args) {

        // kafka consumer settings
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // kafka broker subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC_NAME));

        Thread mainThread = Thread.currentThread();

        // main 스레드 종료시 별도의 thread로 consumer wakeup 메서드 호출
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("wake up call");
//            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                LOGGER.error("main thread dead");
            }
        }));

        pollCommitAsync(consumer);
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> consumer) {
        long count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                LOGGER.info("############## count: {}, ConsumerRecords count: {}", count++, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("record key: {}, value: {}, partition: {}, record_offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }

                try {
                    LOGGER.info("main thread is sleeping {} ms during while loop ", 10000);
                    Thread.sleep(10000L);
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

    private static void pollCommitSync(KafkaConsumer<String, String> consumer) {
        long count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                LOGGER.info("############## count: {}, ConsumerRecords count: {}", count++, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("record key: {}, value: {}, partition: {}, record_offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }

                // 동기적인 commit
                try {
                    if (records.count() > 0) {
                        consumer.commitSync();
                        LOGGER.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            LOGGER.error("wake up exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            consumer.close();
        }
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> consumer) {
        long count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                LOGGER.info("############## count: {}, ConsumerRecords count: {}", count++, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("record key: {}, value: {}, partition: {}, record_offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }

                // 비동기적인 commit
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            LOGGER.error("offsets: {} is not completed, error: {}", offsets, exception);
                        }
                    }
                });
            }
        } catch (WakeupException e) {
            LOGGER.error("wake up exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            consumer.commitSync();
            consumer.close();
        }
    }
}
