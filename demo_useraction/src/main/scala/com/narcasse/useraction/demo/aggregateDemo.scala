package com.narcasse.useraction.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
   
*/

object aggregateDemo {


  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[5]").getOrCreate()

    val data = List((1,4),(2,3),(3,6),(1,5),(3,8),(2,5))

    val rdd: RDD[(Int, Int)] = spark.sparkContext.parallelize(data)

    val res = rdd.aggregateByKey(0l)(
     Math.max(_,_),_+_
    )

    res.collect().foreach(println)

  }

}
