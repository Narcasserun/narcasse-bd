package com.narcasse.useraction.demo

import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.util.{MultiMap, UrlEncoded}

/*
   
*/

object UVcompute {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().appName("UVcompute").master("local[4]").getOrCreate()

    val data = spark.sparkContext.textFile("C:\\javaEE\\github\\narcasse-bd\\demo_useraction\\doc\\nginx_example.txt")

    data.map(_.split("\t")).filter(e=>e(1).contains("product_id")&&e(3).contains("uid")).map(e=>{
      val paramsMap = new MultiMap[String];
      UrlEncoded.decodeTo(e(1), paramsMap, "UTF-8")
      val product_id = paramsMap.getValue("product_id",0)
      val uid= e(3).split("=")(0)
      (uid,product_id)
    }).distinct().map(e=>(e._2,1l)).reduceByKey(_+_).sortBy(e=>e._2).saveAsTextFile("C:\\logs\\nginx_output_uv")

    spark.close()


  }

}
