package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable>  {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileToDBConsumer.class.getName());
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final OrderDBHandler dbHandler;

    public FileToDBConsumer(Properties properties, List<String> topics, OrderDBHandler dbHandler) {
        this.consumer = new KafkaConsumer<>(properties);
        this.topics = topics;
        this.dbHandler = dbHandler;
    }
    public void initConsumer() {
        this.consumer.subscribe(this.topics);
        shutdownHookToRuntime(this.consumer);
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
            } catch(InterruptedException e) { e.printStackTrace();}
        }));

    }

    private void processRecord(ConsumerRecord<K, V> record) {
        OrderDTO orderDTO = makeOrderDTO(record);
        dbHandler.insertOrder(orderDTO);
    }

    private OrderDTO makeOrderDTO(ConsumerRecord<K,V> record) {
        String messageValue = (String)record.value();
        LOGGER.info("###### messageValue:" + messageValue);
        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return new OrderDTO(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], LocalDateTime.parse("2022-07-14 12:09:33", formatter));
    }

    private void processRecords(ConsumerRecords<K, V> records) throws Exception{
        List<OrderDTO> orders = makeOrders(records);
        dbHandler.insertOrders(orders);
    }

    private List<OrderDTO> makeOrders(ConsumerRecords<K,V> records) throws Exception {
        List<OrderDTO> result = new ArrayList<>();

        for(ConsumerRecord<K, V> record : records) {
            OrderDTO orderDTO = makeOrderDTO(record);
            result.add(orderDTO);
        }
        return result;
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
            this.close();
        }
    }

    private void pollCommitViaAsync(long durationMillis) {
        while (true) {
            ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(durationMillis));

            if(records.count() > 0) {
                try {
                    processRecords(records);
                } catch(Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }

            this.consumer.commitAsync((offsets, exception) -> {
                if(exception != null) {
                    LOGGER.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                }
            });
        }
    }

    private void pollCommitViaSync(long durationMillis) throws Exception {
        while (true) {
            ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(durationMillis));
            processRecords(records);
            if (records.count() > 0) {
                this.consumer.commitSync();
                LOGGER.info("commit sync has been called");
            }
        }
    }

    public void close() {
        this.consumer.close();
        this.dbHandler.close();
    }
}
