package com.narcasse.flink.streaming.custom;



/**
 * connect
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 *
 *
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = senv.addSource(new MyParalleSource()).setParallelism(1);
        DataStreamSource<Long> streamSource2 = senv.addSource(new MyParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> map = streamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value;
            }
        });

        SingleOutputStreamOperator<String> map2 = streamSource2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });
        ConnectedStreams<Long, String> streams = map.connect(map2);
        SingleOutputStreamOperator<Object> map1 = streams.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });
        map1.print().setParallelism(1);

        try {
            senv.execute("ConnectStreamDemo");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
