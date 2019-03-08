package com.gupao.bd.trademonitor.mcs.metric

import com.alibaba.fastjson.JSONObject

/**
  * 功能：客群分析（新老客占比）
  **/
case class UserRatio(dt: String, userType: String, userCnt: Int) {
  val metricType = "UserRatio"

  val json: JSONObject = new JSONObject()
  json.put("metric", metricType)
  json.put("userType", userType)
  json.put("userCnt", userCnt)
  json.put("dt", dt)

  override def toString: String = json.toJSONString
}
