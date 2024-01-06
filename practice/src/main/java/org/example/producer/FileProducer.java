package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * key 값을 사용하여 특정 파티션으로 message 전송
 * kafka-topics --bootstrap-server localhost:9092 --create --topic file-topic
 * kafka-console-consumer --bootstrap-server localhost:9092 --group group-topic --topic file-topic --property print.key=true --property print.value=true --from-beginning
 */
public class FileProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(FileProducer.class.getName());

    private static final String DELIMITER = ",";
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

        String filePath = "/Users/kdg/IdeaProjects/study/kafka-study/practice/src/main/resources/pizza_sample.txt";

        // kafka send
        sendFileMessages(kafkaProducer, filePath);

        // kafka close
        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String filePath) {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))){

            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                String[] texts = line.split(DELIMITER);
                String key = texts[0];
                String message = createMessage(texts);

                // send message
                sendMessage(kafkaProducer, key, message.toString());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private static String createMessage(String[] message) {
        StringBuffer result = new StringBuffer();

        for (int i = 1; i < message[1].length(); i++) {
            if (i != (message.length - 1)) {
                result.append(message[i] + ",");
            } else {
                result.append(message[i]);
            }
        }

        return result.toString();
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, message);
        LOGGER.info("key: {}, value: {}", key, message);

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
}
