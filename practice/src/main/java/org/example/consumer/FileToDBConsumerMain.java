package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

/**
 * kafka-topics --bootstrap-server localhost:9092 --create --topic file-topic
 *
 * CREATE TABLE `orders` (
 *   `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'member PK',
 *   `ord_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
 *   `shop_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
 *   `menu_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
 *   `user_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
 *   `phone_number` varchar(100) DEFAULT NULL,
 *   `address` varchar(100) DEFAULT NULL,
 *   `order_time` datetime DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 */
public class FileToDBConsumerMain {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String GROUP_NAME = "file-group";
    private static final String TOPIC_NAME = "file-topic";
    private static final String ENABLE_AUTO_COMMIT = "false";

    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/kafka?serverTimezone=Asia/Seoul&useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "123456789";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);

        OrderDBHandler orderDBHandler = new OrderDBHandler(DRIVER, URL, USER, PASSWORD);
        FileToDBConsumer<String, String> fileToDBConsumer = new FileToDBConsumer<>(props, List.of(TOPIC_NAME), orderDBHandler);
        fileToDBConsumer.initConsumer();

        long durationMillis = 1000L;
        String commitMode = "async";
        fileToDBConsumer.pollConsumes(durationMillis, commitMode);
        fileToDBConsumer.close();
    }
}
