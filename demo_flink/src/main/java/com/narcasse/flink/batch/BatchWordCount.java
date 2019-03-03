package com.narcasse.flink.batch;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> textFile = env.readTextFile("C:\\environment\\大数据\\hello.txt");

        DataSet<Tuple2<String, Integer>> counts = textFile.flatMap(new Tokenizer()).groupBy(0).sum(1);

        try {
            ((AggregateOperator<Tuple2<String,Integer>>) counts).setParallelism(1).print();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] split = value.toLowerCase().split("\\W");
            for (String s : split) {
                if(s.length()>0){
                    out.collect(new Tuple2<String, Integer>(s,1));
                }
            }
        }
    }
}
