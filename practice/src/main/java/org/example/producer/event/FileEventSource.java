package org.example.producer.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileEventSource.class.getName());
    private static final String MODE = "r";
    private static final String DELIMITER = ",";

    private File file;
    private long currentFilePointer;
    private EventHandler eventHandler;
    private boolean keepRunning;
    private long updateInterval;

    public FileEventSource(File file, EventHandler eventHandler, long updateInterval) {
        this.file = file;
        this.currentFilePointer = 0;
        this.keepRunning = true;
        this.eventHandler = eventHandler;
        this.updateInterval = updateInterval;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);

                long length = this.file.length();
                if (length < this.currentFilePointer) {
                    LOGGER.info("file was reset as filePointer is longer then file length");
                    this.currentFilePointer = length;
                } else if (length > this.currentFilePointer) {
                    readAppendAndSend();
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, MODE);
        randomAccessFile.seek(this.currentFilePointer);

        String line = "";
        while ((line = randomAccessFile.readLine()) != null) {
            sendMessage(line);
        }

        // file이 변경되었으므로 currentFilePointer 수정
        this.currentFilePointer = randomAccessFile.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] texts = line.split(DELIMITER);
        String key = texts[0];
        String message = createMessage(texts);

        MessageEvent messageEvent = new MessageEvent(key, message);
        eventHandler.onMessage(messageEvent);
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
}
