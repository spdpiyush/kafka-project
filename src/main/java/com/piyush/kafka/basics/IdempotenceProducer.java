package com.piyush.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created By : Piyush Kumar
 * on 2019-12-28 & 12:24
 */
public class IdempotenceProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(IdempotenceProducer.class.getName());

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Properties For Enabling Exactly Once Feature
        /**
         *  To Enable this : ACKS should be all,
         *      Retries should be > 1 , by default retires value is INT_MAXVALUE
         *      and enable.idempotence should be true
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"100");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"10000");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","Producer With Idempotence");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    logger.info("RecordMetaData : " +
                            "\n Topic : " + recordMetadata.topic() +
                            "\n Partition : " + recordMetadata.partition() +
                            "\n Offset : " + recordMetadata.offset() +
                            "\n TimeStamp : " + recordMetadata.timestamp());
                }else {
                    logger.info("Exception Occurred while producing the record");
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
