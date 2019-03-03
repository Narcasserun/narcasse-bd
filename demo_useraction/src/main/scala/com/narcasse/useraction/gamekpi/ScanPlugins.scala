package com.narcasse.useraction.gamekpi

import org.apache.commons.codec.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object ScanPlugins {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ScanPlugins")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    // 设置检查点
    ssc.checkpoint("hdfs://node01:9000/ck20180109-1")

    // 读取Kafka的参数
    val Array(zkQuorum, group, topics, numThread) =
      Array("node01:2181,node02:2181,node03:2181", "group1", "gamelogs", "2")
    val topicMap: Map[String, Int] = topics.split(",").map((_, numThread.toInt)).toMap
    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )

    // 从Kafka获取数据
    val dStream: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParam, topicMap, StorageLevel.MEMORY_ONLY)

    // 把从Kafka获取的实际数据取出来，key扔掉
    val lines: DStream[String] = dStream.map(_._2)
    // 切分数据
    val splited: DStream[Array[String]] = lines.map(_.split("\t"))
    // 按照条件进行过滤
    val filtered: DStream[Array[String]] = splited.filter(line => {
      val et = line(3)
      val item = line(8)
      et == "11" && item == "强效太阳水"
    })
    // 获取到需要的字段：账号和喝药时间
    val userAndTime: DStream[(String, Long)] =
      filtered.map(line => (line(7), dateFormat.parse(line(12)).getTime))

    // 按照窗口操作进行分组
    val groupedWindow: DStream[(String, Iterable[Long])] =
      userAndTime.groupByKeyAndWindow(Seconds(30), Seconds(20))

    // 为了避免误判，玩家连续喝药的次数大于等于五次才做记录
    val filtered_groupedWindow: DStream[(String, Iterable[Long])] = groupedWindow.filter(_._2.size >= 5)

    // 得到每次喝药的平均时间间隔
    val itemAvgTime: DStream[(String, Long)] = filtered_groupedWindow.mapValues(it => {
      val list = it.toList.sorted // 对喝药的时间进行排序
      val size = list.size // 喝药的次数
      val first = list(0) // 第一次喝药的时间
      val last = list(size - 1) // 最后一次喝药时间

      (last - first) / size // 每次喝药的平均时间间隔
    })
    // 判断是否为开挂用户
    val badUser: DStream[(String, Long)] = itemAvgTime.filter(_._2 < 1000)

    badUser.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val conn: Jedis = JedisConnectionPool.getConnecttion()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          conn.set(user + "_" + currentTime, avgTime.toString)
        })
        conn.close()
      })
    })

    badUser.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
