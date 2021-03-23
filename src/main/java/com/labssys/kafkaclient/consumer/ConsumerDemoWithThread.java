package com.labssys.kafkaclient.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String group = "my-first-application";
        String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(countDownLatch, bootstrapServer, group, topic);

        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException interruptedException) {
            logger.error("Application got interrupted", interruptedException);
        } finally {
            logger.info("Application is closing");
        }
    }
}
