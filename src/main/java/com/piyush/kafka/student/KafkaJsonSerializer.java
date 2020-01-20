package com.piyush.kafka.student;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created By : Piyush Kumar
 * on 2020-01-20 & 14:44
 */
public class KafkaJsonSerializer implements Serializer {

    private Logger logger = LoggerFactory.getLogger(KafkaJsonSerializer.class.getName());
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] value = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            value = objectMapper.writeValueAsBytes(o);
        }catch (Exception e){
            logger.error("Error Occured {}",e.getMessage());
            e.fillInStackTrace();
        }
        return value;
    }


    @Override
    public void close() {

    }
}
