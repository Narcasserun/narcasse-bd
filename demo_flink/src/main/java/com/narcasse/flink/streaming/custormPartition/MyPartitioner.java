package com.narcasse.flink.streaming.custormPartition;

/*

 */

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区数为："+numPartitions);
        if(key%2==0){
            return 0;
        }
        return 1;
    }
}
