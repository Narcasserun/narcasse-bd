package com.narcasse.flink.streaming.custom;

/**
 * split
 *
 * 根据规则把一个数据流切分为多个流
 *
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 *
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
           // Output object for which the output selection should be made.
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
