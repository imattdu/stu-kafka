package com.matt.kafka.producer.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 重写分区
 * @author matt
 * @create 2021-07-22 0:56
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        Integer integer = cluster.partitionCountForTopic(s);
        if (o == null) {
            return 0;
        }
        return o.toString().hashCode() %  integer;
    }

    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> map) {

    }
}
