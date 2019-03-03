package com.narcasse.useraction.gamekpi

import java.text.SimpleDateFormat

import com.narcasse.useraction.utils.{EventType, FilterUtils, TimeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKpi {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val beginTime: Long = TimeUtils(queryTime)
    val endTime: Long = TimeUtils.getCertainDayTime(1)
    val beginTimeForMorrow = TimeUtils.getCertainDayTime(1)
    val endTimeForMorrow = TimeUtils.getCertainDayTime(2)

    // 模板代码
    val conf = new SparkConf()
      .setAppName("GameKpi")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据
    val splitedLog: RDD[Array[String]] =
      sc.textFile("C:\\javaEE\\github\\narcasse-bd\\demo_useraction\\doc\\GameLog.txt").map(_.split("\\|"))

    // 1、使用SimpleDateFormat会涉及到线程安全的问题，此时不建议用
    // 2、SimpleDateFormat和filter过滤的逻辑会经常用到，最好把这些放到一个工具类里
//    splitedLog.filter(_(0) == EventType.LOGIN).filter(x => {
//      val time = x(1)
//      val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
//      val time_long = sdf.parse(time).getTime
//    })

    // 日新增用户数(DNU)
    val dnu: RDD[Array[String]] = splitedLog.filter(fields => {
      FilterUtils.filterByTypeAndTime(fields, EventType.REGISTER, beginTime, endTime)
    })

    // 日活跃用户数：DAU--Daily Active User
    val filterByTime: RDD[Array[String]] = splitedLog.filter(fields => {
      FilterUtils.filterByTime(fields, beginTime, endTime)
    })
    val filterByTypes: RDD[Array[String]] = filterByTime.filter(fields => {
      FilterUtils.filterByTypes(fields, EventType.REGISTER, EventType.LOGIN)
    })
    val dau: RDD[String] = filterByTypes.map(_(3)).distinct()

    // 次日留存
    val dnuTup: RDD[(String, Int)] = dnu.map(fields => (fields(3), 1))
    val day2Login: RDD[Array[String]] = splitedLog.filter(fields => {
      FilterUtils.filterByTypeAndTime(fields, EventType.LOGIN, beginTimeForMorrow, endTimeForMorrow)
    })
    val day2UnameTup: RDD[(String, Int)] = day2Login.map(_(3)).distinct.map((_, 1))
    val morrowKeep: RDD[(String, (Int, Int))] = dnuTup.join(day2UnameTup)

    val brd = sc.broadcast(day2UnameTup.collectAsMap())
    dnuTup.mapPartitions(iter=>{
      var day2tupmap = brd.value
      iter.map(dnuinfo=>(day2tupmap.get(dnuinfo._1).get,dnuinfo._2))
    }).reduceByKey(_+_).collect().foreach(println)

    println("日新增用户：" + dnu.map(_(3)).collect.toBuffer)
    println("日新增用户数：" + dnu.count)
    println("日活跃用户数：" + dau.count())
    println("次日留存用户：" + morrowKeep.collect.toBuffer)
    println("次日留存用户数：" + morrowKeep.count())

    sc.stop()
  }
}
