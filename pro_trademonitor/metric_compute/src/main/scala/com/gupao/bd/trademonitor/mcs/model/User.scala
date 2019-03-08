package com.gupao.bd.trademonitor.mcs.model

import com.alibaba.fastjson.JSONObject

/**
  * 功能：对应user表中的数据
  **/
case class User(json: JSONObject) {
  val data: JSONObject = json.getJSONObject("after")
  val id: String = data.getString("id")
  val userName = data.getString("user_name")
  val city = data.getString("city")

  //  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  //  `user_name` varchar(128) NOT NULL COMMENT '用户名',
  //  `mobile_num` varchar(11) DEFAULT NULL COMMENT '手机号码',
  //  `register_time` datetime DEFAULT NULL COMMENT '注册时间',
  //  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  //  `update_time` datetime DEFAULT NULL COMMENT '更新时间',

  override def toString: String = data.toJSONString
}
