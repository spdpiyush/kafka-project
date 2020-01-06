package com.piyush.kafka.student;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created By : Piyush Kumar
 * on 2020-01-07 & 01:54
 */
public class StudentProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(StudentProducer.class);
        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StudentEntity.class.getName());

        KafkaProducer<String, StudentEntity> producer = new KafkaProducer<String, StudentEntity>(properties);

        StudentEntity student = new StudentEntity();
        student.setId(1);
        student.setName("Piyush Kumar");
        student.setAddress("MV");
        student.setEmail("piyushchamp.pkp@gmail.com");
        student.setContact("90009");

        ProducerRecord<String, StudentEntity>  record = new ProducerRecord<>("student",student);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    logger.info("RecordMetaData : \n" +
                            "Topic : " + recordMetadata.topic() +
                            "\nPartitions : " + recordMetadata.partition() +
                            "\nOffsets : " + recordMetadata.offset() +
                            "\n TimeStamp : " + recordMetadata.timestamp());
                }else {
                    logger.error("Exception : ");
                    e.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
