package com.narcasse.flink.streaming.custormPartition;


import com.narcasse.flink.streaming.custom.MyParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamDemoMyPartitioner {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> streamSource = env.addSource(new MyParalleSource());

        DataStream<Tuple1<Long>> dataStream = streamSource.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {

                return new Tuple1<>(value);
            }
        });

        DataStream<Tuple1<Long>> dataStream1 = dataStream.partitionCustom(new MyPartitioner(), 0);
        SingleOutputStreamOperator<Long> map = dataStream1.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value: " + value);
                return value.getField(0);
            }
        });

        map.print().setParallelism(1);
        try {
            env.execute("StreamDemoMyPartitioner");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
