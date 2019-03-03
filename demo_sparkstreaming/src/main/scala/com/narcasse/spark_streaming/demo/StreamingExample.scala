package com.narcasse.spark_streaming.demo

import org.apache.log4j.{Level, Logger}

trait StreamingExample extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val hostname = "ubuntu1"
  val port = 9999
  val checkpointDir = "C:\\logs\\streaming\\checkpoint"
  val outputDir = "C:\\logs\\streaming\\output"
  val inputDir = "C:\\logs\\streaming\\input"

}
