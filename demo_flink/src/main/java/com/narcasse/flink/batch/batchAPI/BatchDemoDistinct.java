package com.narcasse.flink.batch.batchAPI;

/*

 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class BatchDemoDistinct {
    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> list = Arrays.asList("hello you", "hello me");

        DataSource<String> dataSource = env.fromCollection(list);
        FlatMapOperator<String, String> map = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    System.out.println("接收到的数据是：" + s);
                    out.collect(s);
                }
            }
        });
        try {
            map.distinct().print();
        } catch (Exception e) {

        }

    }
}
