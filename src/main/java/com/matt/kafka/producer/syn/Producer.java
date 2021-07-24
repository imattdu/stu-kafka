package com.matt.kafka.producer.syn;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 同步
 * @author matt
 * @create 2021-07-23 0:43
 */
public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        //kafka 集群，broker-list
        properties.put("bootstrap.servers", "192.168.96.128:9092,192.168.96.129:9092,192.168.96.130:9092");
        //properties.put("acks", "all");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put("retries", 1);
        //批次大小 写入到buffer.memory
        properties.put("batch.size", 16384);
        //等待时间 1ms提交
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);
        // 序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 9; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("first", "cc" + i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            System.out.println(recordMetadata.toString());
                            System.out.println("----------");
                        }
                    });

            // 会阻塞
            //RecordMetadata recordMetadata = future.get();
        }
        // 记得关闭资源
        producer.close();


    }

}
