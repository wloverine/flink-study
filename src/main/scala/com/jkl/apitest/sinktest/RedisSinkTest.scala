package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.streaming.api.scala._

import scala.io.Source

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.readTextFile("sensor.txt")
      .print()

    env.execute("RedisSinkTest")

  }
}
