package com.gupao.bd.trademonitor.mcs.job

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  * 功能 ： kafka/spark/hbase相关的配置信息
  **/
class JobConf(fileName: String) extends Serializable {

  @transient val file = new File(fileName)
  @transient val fis = new FileInputStream(file)
  val properties = new Properties()
  properties.load(fis)
  fis.close()

  val KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("kafka.bootstrap.servers")
  val KAFKA_TOPIC_METRIC = properties.getProperty("kafka.topic.metric")
  val KAFKA_TOPIC_ODS_1 = properties.getProperty("kafka.topic.ods.1")
  val KAFKA_TOPIC_ODS_2 = properties.getProperty("kafka.topic.ods.2")
  val KAFKA_TOPIC_ODS_3 = properties.getProperty("kafka.topic.ods.3")
  val KAFKA_TOPIC_ODS_4 = properties.getProperty("kafka.topic.ods.4")
  val HBASE_ZOOKEEPER_QUORUM = properties.getProperty("hbase.zookeeper.quorum")
  val SPARK_MASTER = properties.getProperty("spark.master")
  val SPARK_CHECKPOINT_DIR = properties.getProperty("spark.checkpoint.dir")
}
