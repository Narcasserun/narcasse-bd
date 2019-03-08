package com.gupao.bd.trademonitor.mcs.metric

import com.alibaba.fastjson.JSONObject

/**
  * 功能：各城市累计注册用户数
  **/
case class RegisterUserCnt(dt: String, city: String, userCnt: Int) {
  val json: JSONObject = new JSONObject()
  json.put("metric", "RegisterUserCnt")
  json.put("city", city)
  json.put("userCnt", userCnt)
  json.put("dt", dt)

  override def toString: String = json.toJSONString
}
