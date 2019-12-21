package com.piyush.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created By : Piyush Kumar
 * on 2019-12-21 & 09:49
 */
public class ConsumerWithAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerWithAssignSeek.class);
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        Long offset = 15L;
        int numberOfMessagesToRead = 5;
        boolean onReading = true;
        int count = 0;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition partition = new TopicPartition(topic,0);

        consumer.assign(Arrays.asList(partition));

        consumer.seek(partition,offset);

        while (onReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record:records){
                count++;
                logger.info("Key : " + record.key() + " Value : " + record.value());
                logger.info("Partition : " + record.partition() + " Offset : " + record.offset());
                if(count >= numberOfMessagesToRead){
                    onReading = false;
                    break;
                }
            }
        }
        logger.info("Application getting Closed");
    }
}
