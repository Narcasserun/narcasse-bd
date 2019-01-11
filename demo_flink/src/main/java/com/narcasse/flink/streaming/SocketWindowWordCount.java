package com.narcasse.flink.streaming;

/*
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 *
 *
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void main(String[] args) {
        int port;
        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        }catch (Exception e){
            System.err.println("no port input,use default port 9000");
            port=9000;
        }
        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "ubuntu1";
        String delimiter = "\n";

        //连接socket获取输入的数据
        DataStreamSource<String> textStream = env.socketTextStream(hostname, port);
        // a a c

        // a 1
        // a 1
        // c 1
        DataStream<WordWithCount> windowCounts = textStream.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word : splits) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count");//在这里使用sum或者reduce都可以
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);
        //这一行代码一定要实现，否则程序不执行
        try {
            env.execute("Socket window count");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static class WordWithCount{
        public String word;
        public long count;
        public  WordWithCount(){}
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
