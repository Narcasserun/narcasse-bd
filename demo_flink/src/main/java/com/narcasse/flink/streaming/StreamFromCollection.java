package com.narcasse.flink.streaming;

/*

 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamFromCollection {
    public static void main(String[] args) {

        //获取flink运行时环境。可用于设置运行参数
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> list = new ArrayList<>();
        list.add(20);
        list.add(12);
        list.add(18);
        list.add(18);

        SingleOutputStreamOperator<Integer> stream = senv.fromCollection(list).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });
        stream.print().setParallelism(1);

        try {
            senv.execute("stream from collection");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
