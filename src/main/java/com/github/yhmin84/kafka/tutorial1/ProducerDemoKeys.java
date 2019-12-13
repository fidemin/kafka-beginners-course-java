package com.github.yhmin84.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // crete Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer <String, String> : key to be string and value to be string
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i=0; i<10; i++) {
                String topic = "first_topic";
                String value = "hello world " + i;
                String key = "id_" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // same key always goes to the same partition!
                logger.info("Key: " + key);

                // send data - asynchronous.
                // consumer doesn't receive any message until buffer is full or flush or close method is not executed.
                producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                    // execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metatdata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);

                    }
                }).get(); // block the .send() to make it synchronous. don't do this in production
            }

            producer.flush();
        }
    }
}
