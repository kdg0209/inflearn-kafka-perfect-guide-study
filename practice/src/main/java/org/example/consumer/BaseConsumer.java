package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K extends Serializable, V extends Serializable> {

    public static final Logger LOGGER = LoggerFactory.getLogger(BaseConsumer.class.getName());

    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;

    public BaseConsumer(Properties properties, List<String> topics) {
        this.consumer = new KafkaConsumer<>(properties);
        this.topics = topics;
    }

    public void initConsumer() {
        this.consumer.subscribe(this.topics);
        shutdownHookToRuntime(this.consumer);
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            if (commitMode.equals("sync")) {
                pollCommitViaSync(durationMillis);
            } else {
                pollCommitViaAsync(durationMillis);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } finally {
            this.consumer.commitSync();
            this.closeConsumer();
        }
    }

    public void closeConsumer() {
        this.consumer.close();
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            })
        );
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        LOGGER.info("record key:{}, partition:{}, offset:{}, value:{}", record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }

    private void pollCommitViaAsync(long durationMillis) {
        while (true) {
            ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(durationMillis));
            processRecords(records);

            this.consumer.commitAsync((offsets, exception) -> {
                if(exception != null) {
                    LOGGER.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                }
            });
        }
    }

    private void pollCommitViaSync(long durationMillis) {
        while (true) {
            ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(durationMillis));

            if(records.count() > 0 ) {
                processRecords(records);
                this.consumer.commitSync();
                LOGGER.info("commit sync has been called");
            }
        }
    }
}
