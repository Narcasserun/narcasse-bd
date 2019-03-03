package com.narcasse.useraction.gamekpi

import java.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  private val config = new JedisPoolConfig

  // 设置最大连接数
  config.setMaxTotal(100)
  // 最大空闲连接数
  config.setMaxIdle(50)
  // 获取连接时检查连接的有效性
  config.setTestOnBorrow(true)

  private val jedisPool: JedisPool = new JedisPool(config, "node04", 6379)

  // 获取连接池的连接
  def getConnecttion() = jedisPool.getResource

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnecttion()
    val keys: util.Set[String] = conn.keys("*")
    println(keys)
  }
}
