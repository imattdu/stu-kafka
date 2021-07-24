package com.matt.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.lang.model.element.VariableElement;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author matt
 * @create 2021-07-24 11:22
 */
public class MyConsumer {

    public static void main(String[] args) {

        // 消费者配置信息
        Properties properties = new Properties();
        // 集群信息
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.128:9092，192.168.96.129:9092，192.168.96.130:9092");
        // 消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        // 自动提交
        // 会从上次消费的地方开始消费
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // 重置offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList("first"));

        while (true) {
            // 长轮询 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + ":" + consumerRecord.value());
            }
        }


    }
}
