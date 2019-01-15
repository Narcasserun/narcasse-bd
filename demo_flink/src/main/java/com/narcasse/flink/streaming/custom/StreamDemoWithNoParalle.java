package com.narcasse.flink.streaming.custom;

/*

 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamDemoWithNoParalle {
    public static void main(String[] args) {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = senv.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<Long> stream = streamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据："+value);
                return value;
            }
        });

        //每两秒处理一次数据
        SingleOutputStreamOperator<Long> sum = stream.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);
        try {
            senv.execute("StreamDemoWithNoParalle");
        } catch (Exception e) {
            e.printStackTrace();

        }

    }
}
