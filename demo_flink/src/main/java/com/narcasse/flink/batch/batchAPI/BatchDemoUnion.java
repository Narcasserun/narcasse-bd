package com.narcasse.flink.batch.batchAPI;

/*

 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class BatchDemoUnion {
    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));


        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"lili"));
        data2.add(new Tuple2<>(2,"jack"));
        data2.add(new Tuple2<>(3,"jessic"));

        DataSource<Tuple2<Integer, String>> d1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> d2 = env.fromCollection(data2);

        UnionOperator<Tuple2<Integer, String>> union = d1.union(d2);
        try {
            union.print();
        } catch (Exception e) {

        }

    }
}
