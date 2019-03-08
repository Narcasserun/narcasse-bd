package com.gupao.bd.trademonitor.mcs.job

import java.util.concurrent.{ExecutorService, Executors}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gupao.bd.trademonitor.mcs.dao.{HBaseClient, KafkaClient}
import com.gupao.bd.trademonitor.mcs.metric._
import com.gupao.bd.trademonitor.mcs.model._
import org.apache.commons.lang3.{StringUtils, Validate}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

/**
  * 功能：从kafka各topic消费数据，完成对应统计指标的实时计算
  **/
class MetricComputeJob(jobConf: JobConf) extends BaseStreamingJob(jobConf) {

  val TOPIC_ODS_1 = jobConf.KAFKA_TOPIC_ODS_1
  val TOPIC_ODS_2 = jobConf.KAFKA_TOPIC_ODS_2
  val TOPIC_ODS_3 = jobConf.KAFKA_TOPIC_ODS_3
  val TOPIC_ODS_4 = jobConf.KAFKA_TOPIC_ODS_4
  val TOPIC_METRIC = jobConf.KAFKA_TOPIC_METRIC
  val KAFKA_BOOTSTRAP_SERVERS = jobConf.KAFKA_BOOTSTRAP_SERVERS
  val HBASE_ZOOKEEPER_QUORUM = jobConf.HBASE_ZOOKEEPER_QUORUM
  val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
  val T_USER_RATIO = "realtime:metric_user_ratio"
  val T_TOTAL_PRICE = "realtime:metric_total_price"
  val T_REGISTER = "realtime:metric_register_user_cnt"
  val T_PRICE_BY_PRD = "realtime:metric_price_by_prd"

  val checkpointDir = jobConf.SPARK_CHECKPOINT_DIR
  override val streamingJobName: String = "MetricComputeJob"

  /**
    * 计算指标
    **/
  def analyze(ssc: StreamingContext, customerList: Broadcast[List[Int]]) = {

    // 按省份统计累计注册人数
    getKafkaRDD(ssc, TOPIC_ODS_1, "MetricJob_" + TOPIC_ODS_1)
      .map(i => JSON.parse(i.value()).asInstanceOf[JSONObject])
      .map(User(_))
      .map(user => (user.city, 1))
      .updateStateByKey((currValues: Seq[Int], prevValueState: Option[Int]) => {
        //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val currentCount = currValues.sum
        // 已累加的值
        val previousCount = prevValueState.getOrElse(0)
        // 返回累加后的结果，是一个Option[Int]类型
        Some(currentCount + previousCount)
      }).foreachRDD((rdd, time) => {

      val dt = DateFormatUtils.format(time.milliseconds, DATE_FORMAT)
      println(">>>>>>>> time:" + dt)

      rdd.foreachPartition(part => {

        val kafkaClient = new KafkaClient(KAFKA_BOOTSTRAP_SERVERS)
        val hbaseClient = new HBaseClient(HBASE_ZOOKEEPER_QUORUM)

        part.foreach(r => {
          val (city, userCnt) = r
          println(s"[ ${city} ] 今日累计注册人数:${userCnt}")
          kafkaClient.produce(TOPIC_METRIC, RegisterUserCnt(dt, city, userCnt).toString)

          val data = new JSONObject()
          data.put("userCnt", userCnt)

          //日期:城市
          val rk = StringUtils.substringBefore(dt, " ") + ":" + city
          hbaseClient.upsert(T_REGISTER, HBaseRecord(rk, data))

        })
      })
    })

    // 过去10分钟内的各产品的成交额
    getKafkaRDD(ssc, TOPIC_ODS_2, "MetricJob_" + TOPIC_ODS_2)
      .map(i => JSON.parse(i.value()).asInstanceOf[JSONObject])
      .map(OrderDetail(_))
      .map(e => (e.pName, e.totalPrice))
      //      .reduceByWindow((a, b) => a + b, Seconds(10), Seconds(10))
      .reduceByKey(_ + _)
      .foreachRDD((rdd, time) => {

        val dt = DateFormatUtils.format(time.milliseconds, DATE_FORMAT)
        println(">>>>>>>> time:" + dt)

        rdd.foreachPartition(part => {

          val kafkaClient = new KafkaClient(KAFKA_BOOTSTRAP_SERVERS)
          val hbaseClient = new HBaseClient(HBASE_ZOOKEEPER_QUORUM)

          part.foreach(r => {
            val (pName, price) = r
            println(s"[ ${pName} ] 过去10分钟的成交额:${price}")
            kafkaClient.produce(TOPIC_METRIC, PriceByPrd(dt, pName, price).toString)

            val data = new JSONObject()
            data.put("price", price)
            //时间:产品
            val rk = StringUtils.substringBefore(dt, ".") + ":" + pName
            hbaseClient.upsert(T_PRICE_BY_PRD, HBaseRecord(rk, data))

          })
        })
      })

    // 累计成交量
    val commonRDD = getKafkaRDD(ssc, TOPIC_ODS_3, "MetricJob_" + TOPIC_ODS_3)
      .map(i => JSON.parse(i.value()).asInstanceOf[JSONObject])
      .map(Order(_))

    commonRDD.map(e => ("total", e.totalPrice))
      .updateStateByKey((currValues: Seq[Double], prevValueState: Option[Double]) => {
        //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val currentCount = currValues.sum
        // 已累加的值
        val previousCount: Double = prevValueState.getOrElse(0)
        // 返回累加后的结果，是一个Option[Int]类型
        Some(currentCount + previousCount)
      }).foreachRDD((rdd, time) => {

      val dt = DateFormatUtils.format(time.milliseconds, DATE_FORMAT)
      println(">>>>>>>> time:" + dt)

      rdd.foreachPartition(part => {

        val kafkaClient = new KafkaClient(KAFKA_BOOTSTRAP_SERVERS)
        val hbaseClient = new HBaseClient(HBASE_ZOOKEEPER_QUORUM)

        part.foreach(r => {
          val (_, totalPrice) = r
          println(s"平台累计成交量:${dt} -> ${totalPrice}")
          kafkaClient.produce(TOPIC_METRIC, TotalPrice(dt, totalPrice).toString)

          val data = new JSONObject()
          data.put("totalPrice", totalPrice)
          //时间
          val rk = StringUtils.substringBefore(dt, ".")
          hbaseClient.upsert(T_TOTAL_PRICE, HBaseRecord(rk, data))

        })
      })
    })

    //客群分析
    val cl = customerList.value
    commonRDD.map(e => (if (cl.contains(e.userId)) "老客" else "新客", 1))
      .updateStateByKey((currValues: Seq[Int], prevValueState: Option[Int]) => {
        //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val currentCount = currValues.sum
        // 已累加的值
        val previousCount = prevValueState.getOrElse(0)
        // 返回累加后的结果，是一个Option[Int]类型
        Some(currentCount + previousCount)
      }).foreachRDD((rdd, time) => {

      val dt = DateFormatUtils.format(time.milliseconds, DATE_FORMAT)
      println(">>>>>>>> time:" + dt)

      rdd.foreachPartition(part => {

        val kafkaClient = new KafkaClient(KAFKA_BOOTSTRAP_SERVERS)
        val hbaseClient = new HBaseClient(HBASE_ZOOKEEPER_QUORUM)

        part.foreach(r => {
          val (userType, userCnt) = r

          println(s"[ ${userType} ] 今日累计成交笔数:${userCnt}")

          kafkaClient.produce(TOPIC_METRIC, UserRatio(dt, userType, userCnt).toString)

          val data = new JSONObject()
          data.put("userCnt", userCnt)
          //日期:用户类型
          val rk = StringUtils.substringBefore(dt, " ") + ":" + userType
          hbaseClient.upsert(T_USER_RATIO, HBaseRecord(rk, data))

        })

      })
    })
  }

  override def process(ssc: StreamingContext) = {
    //老客列表
    val customerList = ssc.sparkContext.broadcast(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    ssc.checkpoint(checkpointDir)
    analyze(ssc, customerList)
  }
}

object MetricComputeJob {
  def main(args: Array[String]): Unit = {
    Validate.isTrue(args != null && !args.isEmpty, "需要通过参数指定配置文件的全路径，配置文件中包含kafka/spark/hbase相关的信息")

    val jobConf: JobConf = new JobConf(args(0))
    val job: MetricComputeJob = new MetricComputeJob(jobConf)

    val pool: ExecutorService = Executors.newFixedThreadPool(1)
    pool.execute(job)
  }
}

