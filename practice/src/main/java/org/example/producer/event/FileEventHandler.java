package org.example.producer.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public final class FileEventHandler implements EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileEventHandler.class.getName());

    private final String topicName;
    private final boolean isSync;
    private final KafkaProducer<String, String> producer;

    public FileEventHandler(String topicName, boolean isSync, KafkaProducer<String, String> producer) {
        this.topicName = topicName;
        this.isSync = isSync;
        this.producer = producer;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, messageEvent.key(), messageEvent.value());

        if (this.isSync) {
            sendMessageViaSync(record);
        } else {
            sendMessageViaAsync(record);
        }
    }

    private void sendMessageViaSync(ProducerRecord<String, String> record) throws ExecutionException, InterruptedException {
        RecordMetadata metadata = this.producer.send(record).get();
        LOGGER.info("############################################################");
        LOGGER.info("record metadata received ");
        LOGGER.info("partition : {}", metadata.partition());
        LOGGER.info("offset : {}", metadata.offset());
        LOGGER.info("has offset : {}", metadata.hasOffset());
        LOGGER.info("timestamp : {}", metadata.timestamp());
        LOGGER.info("############################################################");
    }

    private void sendMessageViaAsync(ProducerRecord<String, String> record) {
        this.producer.send(record, ((metadata, exception) -> {
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
        }));
    }
}
