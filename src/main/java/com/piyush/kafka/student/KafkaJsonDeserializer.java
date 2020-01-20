package com.piyush.kafka.student;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created By : Piyush Kumar
 * on 2020-01-20 & 14:50
 */
public class KafkaJsonDeserializer<T> implements Deserializer {

    private Logger logger = LoggerFactory.getLogger(KafkaJsonSerializer.class);

    private  Class<T> type;
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        T obj = null;
        try {
            obj = objectMapper.readValue(bytes,type);
        }catch (Exception e){
            logger.error("Error Occurred {}",e.getMessage());
            e.fillInStackTrace();
        }
        return obj;
    }



    @Override
    public void close() {

    }
}
