package com.narcasse.kafka.partition;

/*

 */

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.utils.Utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
//自定义分区根据value值进行分区
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if(valueBytes==null){
            throw new InvalidReceiveException("we expect all messages to have customer id as key");
        }
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
        int id;
        int nameSize;
        String name;

        //第一部分：id固定占用4字节
        id = buffer.getInt();

        //第二部分：name的长度
        nameSize = buffer.getInt();

        //第三部分：根据name的长度取值
        byte[] nameBytes = new byte[nameSize];
        buffer.get(nameBytes);
        try {
            name = new String(nameBytes, "UTF-8");
            if(name.contains("1")){
                return numPartitions-1;
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        // 这里是因为kafka是单节点
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 0);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
