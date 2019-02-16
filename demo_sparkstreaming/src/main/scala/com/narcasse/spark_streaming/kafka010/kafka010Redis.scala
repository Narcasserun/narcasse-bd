package com.narcasse.spark_streaming.kafka010

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/*

 */
object kafka010Redis {
   def main(args: Array[String]) {

     val ssc = createContext()
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
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
        ,"/opt/jars/kafka-clients-0.10.2.2.jar"
        ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //    使用broker和topic创建DirectStream
    val topicsSet = "test1".split(",").toSet
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->"test1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean))
    // 没有接口提供 offset
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    // 会报错的哦,object not serializable (class: org.apache.kafka.clients.consumer.ConsumerRecord
    //     messages.checkpoint(Seconds(20))
    // 为啥不担心并发访问redis同一rowkey的问题
    messages.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).
      foreachRDD(rdd=>{
        rdd.foreachPartition(partition => {
          if(partition.nonEmpty){
            val jedis = new Jedis("mt-mdh.local", 6379)
            partition.foreach{case (key:String,value:Int) =>
              val lastVal = jedis.get(key)
              if(lastVal!=null){
                val res = lastVal.toInt + value
                jedis.set(key,String.valueOf(res))
                jedis.expire(key,20)
              }else{
                jedis.set(key,String.valueOf(value))
                jedis.expire(key,20)
              }
            }
            jedis.close()
          }
        })
        println("============> batch end !")
      })
    ssc
  }
}
