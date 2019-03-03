package com.narcasse.user_action;

/*

 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AggregateDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("AggregateDemo").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> abk = Arrays.asList(
                new Tuple2<String, Integer>("class1", 1),
                new Tuple2<String, Integer>("class1", 2),
                new Tuple2<String, Integer>("class1", 3),
                new Tuple2<String, Integer>("class2", 4),
                new Tuple2<String, Integer>("class2", 5),
                new Tuple2<String, Integer>("class2", 6));
        JavaPairRDD<String, Integer> abkrdd = sc.parallelizePairs(abk, 2);
        abkrdd.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {
                    @Override
                    public Iterator<String> call(Integer s,
                                                 Iterator<Tuple2<String, Integer>> v)
                            throws Exception {
                        List<String> li = new ArrayList<>();
                        while (v.hasNext()) {
                            li.add("data：" + v.next() + " in " + (s + 1) + " " + " partition");
                        }
                        return li.iterator();
                    }
                }, true).foreach(m -> System.out.println(m));
        JavaPairRDD<String, Integer> abkrdd2 = abkrdd.aggregateByKey(0,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer s, Integer v) throws Exception {
                        System.out.println("seq:" + s + "," + v);
                        return Math.max(s, v);
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer s, Integer v) throws Exception {
                        System.out.println("com:" + s + "," + v);
                        return s + v;
                    }
                });

        abkrdd2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println("c:" + s._1 + ",v:" + s._2);
            }
        });

    }
}
