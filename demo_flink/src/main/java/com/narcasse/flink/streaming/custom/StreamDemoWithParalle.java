package com.narcasse.flink.streaming.custom;

/*

 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamDemoWithParalle {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> streamSource = senv.addSource(new MyParalleSource()).setParallelism(1);
        DataStreamSource<Long> streamSource2 = senv.addSource(new MyParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> text = streamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收的数据是："+value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {

                return value % 2 == 0;
            }
        });
        SingleOutputStreamOperator<Long> text2 = streamSource2.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("source2接受的数据是：" + value);
                return value;
            }
        });
        DataStream<Long> union = text.union(text2);
        union.timeWindowAll(Time.seconds(1)).sum(0).print().setParallelism(1);

        try {
            senv.execute("StreamDemoWithParalle");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
