package com.labssys.kafkaclient.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        // Create the Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.01:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Hello world");

        // Send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (exception == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition:" + recordMetadata.partition() + "\n"  +
                        "Offset:" + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", exception);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
