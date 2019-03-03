package com.narcasse.useraction.demo

import org.apache.spark.sql.SparkSession

/*
   
*/

object Testjoin {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[5]").getOrCreate()

    var tup1rdd=spark.sparkContext.parallelize(List(
      Tuple2("a",1),
      Tuple2("b",1),
      Tuple2("c",1),
      Tuple2("d",1),
      Tuple2("e",1),
      Tuple2("f",1)
    ))

    var tup2rdd=spark.sparkContext.parallelize(List(
      Tuple2("a",1),
      Tuple2("b",1),
      Tuple2("c",1),
      Tuple2("g",1)
    ))
    println(tup1rdd.join(tup2rdd).collect().toBuffer)
    println(tup1rdd.join(tup2rdd).count())
  }

}
