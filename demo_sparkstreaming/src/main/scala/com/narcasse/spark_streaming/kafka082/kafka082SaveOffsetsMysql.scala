package com.narcasse.spark_streaming.kafka082

import java.sql.Timestamp

import com.narcasse.spark_streaming.utils.MysqlUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable.HashMap


object kafka082SaveOffsetsMysql {
   def main(args: Array[String]) {
      //    创建一个批处理时间是2s的context 要增加环境变量
      val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
        .set("yarn.resourcemanager.hostname", "mt-mdh.local")
        .set("spark.executor.instances","2")
        .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
          ,"/opt/meitu/bigdata/lib/mysql-connector-java-5.1.47.jar"
          , "/opt/jars/kafka_2.11-0.8.2.2.jar"
          , "/opt/jars/kafka-clients-0.8.2.2.jar"
          , "/opt/jars/metrics-core-2.2.0.jar"
          , "/opt/jars/spark-streaming-kafka-0-8_2.11-2.3.1.jar"
        ))
      val ssc = new StreamingContext(sparkConf, Seconds(5))

      //    使用broker和topic创建DirectStream
      val kafkaParams = Map("metadata.broker.list" -> "mt-mdh.local:9092"
        , "zookeeper.connect" -> "localhost:2181/kafka082"
        , "zookeeper.connection.timeout.ms" -> "10000")
      val topicsSet = Set("page_visits")


     val tpMap = getLastOffsets("test","select offset  from res where id = (select max(id) from res)")
     var messages:InputDStream[(String, String)]  = null
       if(tpMap.nonEmpty){
         messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String,String)](
         ssc, kafkaParams, tpMap.toMap, (mmd: MessageAndMetadata[String, String]) =>(mmd.key, mmd.message))
     }else{
         kafkaParams + ("auto.offset.reset" -> "largest")
         messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
       }


     messages.foreachRDD(rdd=>{

       // 1. 对于规则匹配类型，可以直接在每次结果输出的时候带上offset，然后展示的时候简单根据offset去重即可。
       val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//       val bc = ssc.sparkContext.broadcast(jSONArray.toString())
        val offset = offsetRanges2json(offsetRanges).toString()
       println("========= > "+offset)

       if(false){
         rdd.map(_._2.toInt).filter(_ % 5 == 3).foreachPartition { iter =>
           val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
           println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
         }
       }else{
         // 2. 对于聚合操作，那就批处理提交。
         rdd.map(_._2)
           .flatMap(_.split(" "))
           .map((_,1L))
           .reduceByKey(_+_)
           .repartition(1)
           .foreachPartition(partition=>{
             Class.forName("com.mysql.jdbc.Driver");
             if(partition.nonEmpty){

               val conn = MysqlUtil.getConnection("test")

               conn.setAutoCommit(false)
               val psts = conn.prepareStatement("INSERT INTO res (word,count,offset,time) VALUES (?,?,?,?)")
               partition.foreach { case(word:String,count:Long)=>
                 psts.setString(1,word)
                 psts.setLong(2,count)
                 psts.setString(3,offset)
                 psts.setTimestamp(4,new Timestamp(System.currentTimeMillis()))
                 psts.addBatch()
               }
               psts.executeBatch()
               conn.commit()
               psts.close()
               conn.close()
             }
           })
       }
     })
      //    启动流
      ssc.start()
      ssc.awaitTermination()
    }

  def offsetRanges2json(arr :Array[OffsetRange]):JSONArray = {
    val jSONArray = new JSONArray()
    arr.foreach(offsetRange=>{
      val jsonObject = new JSONObject()
      jsonObject.accumulate("partition",offsetRange.partition)
      jsonObject.accumulate("fromOffset",offsetRange.fromOffset)
      jsonObject.accumulate("untilOffset",offsetRange.untilOffset)
      jsonObject.accumulate("topic",offsetRange.topic)
      jSONArray.add(jsonObject)
    })
    return  jSONArray
  }

  def getLastOffsets(db : String,sql:String): HashMap[TopicAndPartition, Long] = {
    val conn = MysqlUtil.getConnection(db)
    val psts = conn.prepareStatement(sql)
    val res = psts.executeQuery
    var tpMap : HashMap[TopicAndPartition, Long]  = scala.collection.mutable.HashMap[TopicAndPartition, Long]()
    while (res.next) {
      val o = res.getString(1)
      val jSONArray = JSONArray.fromObject(o)
      jSONArray.toArray().foreach(offset=>{
        val json = JSONObject.fromObject(offset)
        val topicAndPartition = TopicAndPartition(json.getString("topic"), json.getInt("partition"))
        tpMap.put(topicAndPartition,json.getLong("untilOffset"))
      })

    }
    psts.close()
    conn.close()
    return tpMap
  }
}
