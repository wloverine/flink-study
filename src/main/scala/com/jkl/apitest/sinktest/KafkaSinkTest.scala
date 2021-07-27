package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.readTextFile("/Users/daryl/IdeaProjects/flink-study/src/main/resources/sensor.txt")
      .map(data => {
        val fields = data.split(",")
        SensorReading(fields(0), fields(1).toLong, fields(2).toDouble).toString
      })

//    dataStream.writeToSocket("localhost",9999,new SimpleStringSchema())
    dataStream.print()

    env.execute("KafkaSinkTest")
  }
}
