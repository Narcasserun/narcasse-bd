package com.narcasse.useraction.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.util.{MultiMap, UrlEncoded}

/*
   
*/

object PVcompute {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().appName("PVcompute").master("local[4]").getOrCreate()
    val data: RDD[String] = spark.sparkContext
      .textFile("C:\\javaEE\\github\\narcasse-bd\\demo_useraction\\doc\\nginx_example.txt")
    //1.首先判断每一条日志里是否包含pid
    data.map(_.split("\t")).filter(e=>e(1).contains("product_id")).map(e=>{
      val paramsMap = new MultiMap[String];
      UrlEncoded.decodeTo(e(1), paramsMap, "UTF-8")
      val productId = paramsMap.getValue("product_id", 0)
      (productId,1l)
    }).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile("C:\\logs\\nginx_output")

    spark.close()

  }

}
