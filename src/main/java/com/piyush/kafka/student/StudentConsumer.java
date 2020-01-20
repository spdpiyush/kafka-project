package com.piyush.kafka.student;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Created By : Piyush Kumar
 * on 2020-01-20 & 15:20
 */
public class StudentConsumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(StudentConsumer.class);
        String topicName = "student";
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "third_application";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaConsumer<String, StudentEntity> consumer = new KafkaConsumer<>(properties,new StringDeserializer(),new KafkaJsonDeserializer<StudentEntity>(StudentEntity.class));

        consumer.subscribe(Collections.singleton("student"));

        while (true){
            ConsumerRecords<String, StudentEntity> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, StudentEntity> record: records){
                logger.info("Key : {}, Value : {]", record.key(), record.value());
                logger.info("Partitions : {} , Offsest : {} ",record.partition(), record.offset());
            }
        }

    }
}
