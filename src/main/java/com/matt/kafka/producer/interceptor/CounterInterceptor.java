package com.matt.kafka.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author matt
 * @create 2021-07-25 0:37
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int success = 0;
    private int error = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            error++;
        } else {
            success++;
        }
    }

    @Override
    public void close() {
        System.out.println("success:" + success);
        System.out.println("error:" + error);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
