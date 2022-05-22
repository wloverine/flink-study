package com.jkl.scala.sinktest

import com.jkl.scala.sourcetest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

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

    //写入socket流
//    dataStream.writeToSocket("localhost", 9999, new SimpleStringSchema())

    //写入到文件
/*    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("/Users/daryl/IdeaProjects/flink-study/src/main/resources/res.txt"),
      new SimpleStringEncoder[String]("utf-8")
    ).build())*/

    //写入kafka
    dataStream.addSink(new FlinkKafkaProducer[String]("localhost:9092","sensor",new SimpleStringSchema()))

    env.execute("KafkaSinkTest")
  }
}
