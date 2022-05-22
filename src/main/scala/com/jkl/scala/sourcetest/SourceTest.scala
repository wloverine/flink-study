package com.jkl.scala.sourcetest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

//定义样例类，温度传感器
case class SensorReading(
                          id: String,
                          timestamp: Long,
                          temperature: Double
                        )

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从集合中获取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1),
    )

    val stream1 = env.fromCollection(dataList)


    //2.从文件中获取数据
    //env.readTextFile("")

    //3.从kafka中获取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(),properties))

    stream3.print()
    env.execute("SourceTest")

  }
}
