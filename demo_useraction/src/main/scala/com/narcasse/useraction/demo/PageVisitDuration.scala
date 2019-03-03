package com.narcasse.useraction.demo

import java.util.Locale

import com.narcasse.useraction.utils.DateUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.util.{MultiMap, UrlEncoded}

/*
   
*/

object PageVisitDuration {
  def main(args: Array[String]): Unit = {


    val spark =SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[4]").getOrCreate()

    val data = spark.sparkContext
      .textFile("C:\\javaEE\\github\\narcasse-bd\\demo_useraction\\doc\\nginx_example.txt")

    //首先清洗日志
    var CleanRdd = data.map(_.split("\t")).filter(e=>e(1).contains("product_id")&&e(3).contains("uid")).map(e=>{
      val paramsMap = new MultiMap[String];
      UrlEncoded.decodeTo(e(1), paramsMap, "UTF-8")
      val product_id = paramsMap.getValue("product_id",0)
      val uid = e(3).split("=")(1)
      val timestamp = e(4)
      (uid,(product_id,timestamp))
    })
    //转化时间为long类型
    val parseTime2LongRdd = CleanRdd.mapPartitions(partition=>{
      //解析时间
      val SOURCE_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
      partition.map(e=>{
        val timestampdate = SOURCE_TIME_FORMAT.parse(e._2._2).getTime
        (e._1,(e._2._1,timestampdate))
      })
    })
    //把时间装进集合进行排序
    // 根据uid进行分组
    val pageVisitedDurationRDD =parseTime2LongRdd.groupByKey().flatMap(line=>{
      val uid =line._1
      // 对时间集合进行排序
      val timecollection = line._2.toList.sortBy(_._2)
      for (i <- 0 until(timecollection.length-1)) yield {
        val pageid =timecollection(i)._1
        val begintime = timecollection(i)._2
        val endtime = timecollection(i+1)._2
        val difference=endtime-begintime/1000
        (uid,(pageid,difference))
      }
    })
    // Only Debug
    pageVisitedDurationRDD.collect().foreach(println)

    val avgResultRDD = pageVisitedDurationRDD
            .map(pageVisitedTuple => (pageVisitedTuple._2._1, pageVisitedTuple._2._2))
      .aggregateByKey((0L, 0L))((acc, value) => (acc._1 + value, acc._2 + 1L),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(x => (x._1.toDouble / x._2.toDouble))

    // Only Debug
    avgResultRDD.collect().foreach(println)

    spark.close()

  }

}
