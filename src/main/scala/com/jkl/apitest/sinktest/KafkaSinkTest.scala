package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.readTextFile(path)
      .map(data => {
        val fields = data.split(",")
        SensorReading(fields(0), fields(1).toLong, fields(2).toDouble).toString
      })

    //    dataStream.writeToSocket("localhost",9999,new SimpleStringSchema())
    dataStream.print()

    env.execute("KafkaSinkTest")
  }
}
