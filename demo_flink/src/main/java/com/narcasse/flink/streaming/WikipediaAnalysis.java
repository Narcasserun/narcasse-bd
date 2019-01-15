package com.narcasse.flink.streaming;

/*

 */

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {
    public static void main(String[] args) {

        //这可用于设置执行参数并创建从外部系统读取的源
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //百科全书在线聊天系统
        DataStreamSource<WikipediaEditEvent> source = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits  = source.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent value) throws Exception {
                return value.getUser();
            }
        });

        //5.聚合当前窗口中相同用户名的事件，最终返回一个tuple2<user，累加的ByteDiff>
        DataStream<Tuple2<String, Long>> result = keyedEdits.timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> createAccumulator() {
                return new Tuple2<>("", 0l);
            }

            @Override
            public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                return new Tuple2<>(value.getUser(), value.getByteDiff() + accumulator.f1);
            }

            @Override
            public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                return accumulator;
            }

            @Override
            public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        });
        result.print();
        try {
            see.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
