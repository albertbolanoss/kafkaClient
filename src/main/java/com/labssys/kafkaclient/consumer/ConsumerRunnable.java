package com.labssys.kafkaclient.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {
    private CountDownLatch countDownLatch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    public ConsumerRunnable(CountDownLatch countDownLatch, String bootstrapServers, String groupId, String topic) {
        this.countDownLatch = countDownLatch;

        // Create the Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //or latest

        // Create the Consumer
        this.consumer = new KafkaConsumer(properties);

        // Subscribe the consumer
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("key: " + record.key() + ", value: " + record.value());
                    logger.info("partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        } catch (WakeupException wakeupException) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            countDownLatch.countDown();
        }
    }

    public void shutdown() {
        // the consumer method is a special method to interrupt  consumer.poll()
        // it will throw the exception WakeupException
        consumer.wakeup();
    }
}
