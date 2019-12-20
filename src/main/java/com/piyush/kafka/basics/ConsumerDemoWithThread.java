package com.piyush.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created By : Piyush Kumar
 * on 2019-12-20 & 18:12
 */
public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "third_application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerThread(latch,topic,bootstrapServer,groupId);

        Thread thread = new Thread(myConsumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught ShutDown hook");
            ((ConsumerThread)myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application Closed");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("Closing the Application");
        }

    }

    public static class ConsumerThread implements Runnable{

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;



        public ConsumerThread(CountDownLatch latch,
                              String topic,
                              String bootstrapServer,
                              String groupId){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }


        @Override
        public void run() {
           try {
               while (true){
                   ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                   for (ConsumerRecord<String, String> record : records){
                       logger.info("Key : {} , Value {}",record.key(),record.value());
                       logger.info("Partition : {} , Offset : {}",record.partition(),record.offset());
                   }
               }
           }catch (WakeupException e){
               logger.info("Received ShutDown Signal");
           }finally {
               consumer.close();
               // tell our main Code that we are done with the consumer
               latch.countDown();
           }

        }

        public void shutDown(){

            //To Interrupt the consumer.poll() method and throw Wakeup Exception
            consumer.wakeup();
        }
    }
}
