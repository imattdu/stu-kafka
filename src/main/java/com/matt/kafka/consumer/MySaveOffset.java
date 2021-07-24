package com.matt.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author matt
 * @create 2021-07-24 16:26
 */
public class MySaveOffset {

    private static Map<TopicPartition, Long> currentOffset = new
            HashMap<>();

    public static void main(String[] args) {

        // 消费者配置信息
        Properties properties = new Properties();
        // 集群信息
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.128:9092，192.168.96.129:9092，192.168.96.130:9092");
        // 消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        // 自动提交
        // 会从上次消费的地方开始消费
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // 重置offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            /**
             * 功能：重新分配之前调用
             * @author matt
             * @date 2021/7/24
             * @param partitions
             * @return void
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);

            }

            // 重新分配之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 清空offset
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));//
                    // 定位到最近提交的 offset 位置继续消费
                }
            }
        });

        while (true) {
            // 长轮询 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + ":" + consumerRecord.value());
            }
            //异步提交

            commitOffset(currentOffset);
        }


    }


    //获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long>
                                             currentOffset) {
    }
}
