package com.jkl.apitest.windowtest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val textDataStream = env.socketTextStream("localhost", 9999)
      .map(x => {
        val fields = x.split(" ")
        (fields(0), fields(1).toLong)
      })

    val textWithEventTimeDstream = textDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(1000)) {
        override def extractTimestamp(element: (String, Long)): Long = {
          element._2
        }
      })

    textWithEventTimeDstream.keyBy(_._1)
    

    env.execute()


  }
}
