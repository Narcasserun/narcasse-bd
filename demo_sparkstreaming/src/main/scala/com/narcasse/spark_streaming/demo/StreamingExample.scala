package com.narcasse.spark_streaming.demo

import org.apache.log4j.{Level, Logger}

trait StreamingExample extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val hostname = "localhost"
  val port = 9999
  val checkpointDir = "spark_streaming/checkpoint"
  val outputDir = "spark_streaming/output"
  val inputDir = "spark_streaming/input"

}
