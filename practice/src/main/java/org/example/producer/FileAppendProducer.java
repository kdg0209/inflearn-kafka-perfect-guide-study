package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.producer.event.EventHandler;
import org.example.producer.event.FileEventHandler;
import org.example.producer.event.FileEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * key 값을 사용하여 특정 파티션으로 message 전송
 * kafka-topics --bootstrap-server localhost:9092 --create --topic file-topic
 * kafka-console-consumer --bootstrap-server localhost:9092 --group file-group --topic file-topic --property print.key=true --property print.value=true --from-beginning
 */
public class FileAppendProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileAppendProducer.class.getName());

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String TOPIC_NAME = "file-topic";

    public static void main(String[] args) {
        // 1. kafkaProducer configuration setting
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. kafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        boolean isSync = false;
        long updateInterval = 10000L;
        String filePath = "/Users/kdg/IdeaProjects/study/kafka-study/practice/src/main/resources/pizza_append.txt";
        EventHandler eventHandler = new FileEventHandler(TOPIC_NAME, isSync, kafkaProducer);
        FileEventSource fileEventSource = new FileEventSource(new File(filePath), eventHandler, updateInterval);

        Thread thread = new Thread(fileEventSource);
        thread.start();

        try {
            thread.join();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
