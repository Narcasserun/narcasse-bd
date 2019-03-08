package com.gupao.bd.trademonitor.mcs.model

import java.util.Date

import com.alibaba.fastjson.JSONObject

/**
  * 功能：代表一条HBase记录
  **/
case class HBaseRecord(rowKey: String, data: JSONObject, version: Long = new Date().getTime) {

}
