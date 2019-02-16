package com.narcasse.spark_streaming.kafka010

import java.util.Properties


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.ui.WarningListener
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.spark.sql.functions.{asc_nulls_last, desc_nulls_last, grouping_id, lit, month, sum, year,col,struct,expr}


/*

 */
object kafka010Alluxio {
   def main(args: Array[String]) {

     val ssc =createContext
      //    启动流
      ssc.start()
      ssc.awaitTermination()
    }

  def createContext()
  : StreamingContext = {


    //    创建一个批处理时间是2s的context 要增加环境变量
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances","2")
      .set("spark.default.parallelism","4")
      .set("spark.sql.shuffle.partitions","4")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
        ,"/opt/jars/kafka-clients-0.10.2.2.jar"
        ,"/opt/jars/kafka_2.11-0.10.2.2.jar"
        ,"/opt/jars/alluxio-1.7.1-client.jar"
        ,"/opt/meitu/bigdata/lib/mysql-connector-java-5.1.47.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val spark = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)

    
    val listener = new WarningListener(ssc)
    ssc.addStreamingListener(listener)

    //    使用broker和topic创建DirectStream
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->"alluxioTest",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean))
    // 没有接口提供 offset
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("alluxioTest"), kafkaParams))
    // 简单测试去重所有的key，使得结果只会每个key保存一个值。
    var flag =true
    messages.map(_.value()).flatMap(_.split(" "))
      .foreachRDD(rdd=>{
        println("========= > " +listener.numUnprocessedBatches)
        import spark.implicits._
        val wcdf = rdd.map{case(word :String)=>
          wc(word)
        }.toDF.distinct()
        wcdf.createOrReplaceTempView("currentTable")

        //第一次启动
        if(flag){
          flag= false //避免多次进入

          val fromMysql = spark.read.option("user", "root")
            .option("password", "mt2018@#").jdbc("jdbc:mysql://localhost:3306/test","alluxio",new Properties())
//          mysql中数据不存在，直接插入最新收到的一批数据到mysql和alluxio
          if(fromMysql.rdd.isEmpty()){
            wcdf.repartition(1).write.mode("append").option("user", "root")
              .option("password", "mt2018@#").jdbc("jdbc:mysql://localhost:3306/test","alluxio",new Properties()) //不能用coalesce
            wcdf.repartition(1).write.mode("overwrite").parquet("alluxio://localhost:19998/wc/")
          }else{
//            mysql 状态存在 进行状态合并，然后更新mysql和alluxio
            fromMysql.createOrReplaceTempView("lastTable")
            wcdf.createOrReplaceTempView("currentTable")

            val leftJoinRes = "select currentTable.word as word,lastTable.word as tmp from currentTable left outer join lastTable on (currentTable.word = lastTable.word)"
            val res = spark.sql(leftJoinRes)

            res.select(res.col("word")).filter(col("tmp").isNull).drop("tmp").
              repartition(1)
              .write.mode("append").option("user", "root")
              .option("password", "mt2018@#").jdbc("jdbc:mysql://localhost:3306/test","alluxio",new Properties())

            fromMysql.union(wcdf).distinct().repartition(1).write.mode("overwrite").parquet("alluxio://localhost:19998/wc/")
          }
        }else{
//          从alluxio中读取mysql状态
          val last = spark.read.parquet("alluxio://localhost:19998/wc/")
          last.cache().count() //加速缓存，如果有多个action使用到该dataframe就不需要，否则的话不count会抛错误，插入mysql不能算对last的action
          last.createOrReplaceTempView("lastTable")

//          状态合并并选出新增的key插入mysql，同事更新alluxio
          val leftJoinRes = "select currentTable.word as word,lastTable.word as tmp from currentTable left outer join lastTable on (currentTable.word = lastTable.word)"
          val res = spark.sql(leftJoinRes)

            res.select(res.col("word")).filter(col("tmp").isNull).drop("tmp").
              repartition(1)
              .write.mode("append").option("user", "root")
              .option("password", "mt2018@#").jdbc("jdbc:mysql://localhost:3306/test","alluxio",new Properties())

          last.union(wcdf).distinct().repartition(1).write.mode("overwrite").parquet("alluxio://localhost:19998/wc/")
          last.unpersist(true)
          spark.catalog.dropTempView("lastTable")
        }
      })
    ssc
  }
  case class wc(word:String)

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
