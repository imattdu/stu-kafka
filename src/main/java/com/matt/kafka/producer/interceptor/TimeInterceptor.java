package com.matt.kafka.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author matt
 * @create 2021-07-25 1:09
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    private int success;
    private int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {

        String value = producerRecord.value();
        value = System.currentTimeMillis() + value;
        ProducerRecord<String, String> resProducerRecord = new ProducerRecord<String, String>(
                producerRecord.topic(),producerRecord.partition(), producerRecord.timestamp(),
                producerRecord.key(),value,producerRecord.headers()
        );
        return resProducerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

