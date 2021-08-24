package com.kishore.kamal.kafkae2e.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private double splitRatio ;
    private String specialKey;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object o1, byte[] bytes1, Cluster cluster) {
        int noOfPartition = cluster.partitionCountForTopic(topic);
        int sp = (int) (noOfPartition * splitRatio);
        if(key == null || ! (key instanceof String))
            throw new InvalidRecordException("Invalid Key");
        int partition =0;
        if(((String)key).equals(specialKey)){
            partition = Utils.toPositive(Utils.murmur2((keyBytes))) % sp;
        }else{
            partition = Utils.toPositive(Utils.murmur2(keyBytes)) % (noOfPartition-sp) + sp;
        }
        System.out.println("Partition selected :" + partition);
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        splitRatio = 0.5;
        specialKey = map.get("special.key").toString();
    }
}
