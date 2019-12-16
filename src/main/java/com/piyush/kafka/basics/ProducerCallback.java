package com.piyush.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created By : Piyush Kumar
 * on 2019-11-24
 */
public class ProducerCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);
    public static void main(String[] args){

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> record = new ProducerRecord<String, String>("second_topic","HI_KAFKA_CALLBACK_NEW");

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    logger.info("Record MetaData : \n" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "TimeStamp : " + recordMetadata.timestamp());
                }else {
                    logger.error("Error Occurred While Producing Record {}",e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
