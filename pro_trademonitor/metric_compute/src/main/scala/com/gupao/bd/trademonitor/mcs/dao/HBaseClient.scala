package com.gupao.bd.trademonitor.mcs.dao

import java.util.{ArrayList => jArrayList}

import com.gupao.bd.trademonitor.mcs.model.HBaseRecord
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._

/**
  * 功能：执行对HBase对CRUD操作
  **/
class HBaseClient(zkQuorum: String) {

  val hbaseConfig = HBaseConfiguration.create()
  hbaseConfig.set("hbase.zookeeper.quorum", zkQuorum)
  val hbaseConn: Connection = ConnectionFactory.createConnection(hbaseConfig)

  val COL_FAMILY = "info".getBytes

  def upsert(tableName: String, record: HBaseRecord) = {
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(record.rowKey.getBytes, record.version)

    record.data.foreach {
      row => {
        val (key, value) = row
        //ignore null value
        if (value != null) {
          put.addColumn(COL_FAMILY, key.getBytes(), Bytes.toBytes(value.toString))
        }
      }
    }
    table.put(put)
    table.close()
  }
}