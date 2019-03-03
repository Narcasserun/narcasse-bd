package com.narcasse.flink.batch.batchAPI;

/*

 */

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BatchDemoMapPartition {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<String> list = Arrays.asList("hello you", "hello me", "hello she");
        DataSource<String> dataSource = env.fromCollection(list);

//        dataSource.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                //获取数据库连接--注意，此时是每过来一条数据就获取一次链接
//                //处理数据
//                //关闭连接
//                return value;
//            }
//        });

        MapPartitionOperator<String, String> mapPartition = dataSource.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //获取数据库连接--注意，此时是一个分区的数据获取一次连接【优点，每个分区获取一次链接】
                //values中保存了一个分区的数据
                //处理数据
                Iterator<String> iterator = values.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    String[] split = next.split("\\W+");
                    for (String s : split) {
                        out.collect(s);
                    }
                }
                //关闭链接
            }
        });
        try {
            mapPartition.print();
        } catch (Exception e) {

        }

    }
}
