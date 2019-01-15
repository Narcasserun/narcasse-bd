package com.narcasse.flink.streaming.custom;

/*

 */

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamDemo {
    public static void main(String[] args) {

        //set up execute envioment
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = senv.addSource(new MyParalleSource()).setParallelism(1);
        //把流按照就行进行区分
        SplitStream<Long> split = streamSource.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("mic");
                } else {
                    list.add("narcasse");
                }
                return list;
            }
        });
        //选择一个或者多个切分后的流

        DataStream<Long> mic = split.select("mic");
        mic.print().setParallelism(1);

        try {
            senv.execute("SplitStreamDemo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
