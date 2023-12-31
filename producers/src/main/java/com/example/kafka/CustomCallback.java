package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    public static final Logger LOGGER = LoggerFactory.getLogger(CustomCallback.class.getName());

    private final int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exception.printStackTrace();
        } else {
            LOGGER.info("############################################################");
            LOGGER.info("record metadata received ");
            LOGGER.info("seq : {}", seq);
            LOGGER.info("partition : {}", metadata.partition());
            LOGGER.info("offset : {}", metadata.offset());
            LOGGER.info("has offset : {}", metadata.hasOffset());
            LOGGER.info("timestamp : {}", metadata.timestamp());
            LOGGER.info("############################################################");
        }
    }
}
