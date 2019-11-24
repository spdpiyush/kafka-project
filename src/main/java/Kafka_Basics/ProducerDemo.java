package Kafka_Basics;
/**
 * Created By : Piyush Kumar
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args){
        String bootstrapServer = "127.0.0.1:9092";

        //Producer Properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Creating a Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Creating ProducerRecord

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hi_New_to_Kafka_time");

        // Send record - asynchronous
        producer.send(record);

        producer.flush();
        producer.close();
    }
}
