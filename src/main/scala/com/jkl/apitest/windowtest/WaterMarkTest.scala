package com.jkl.apitest.windowtest

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.DateTime

import java.time.Duration


object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000)

    val socketStream = env.socketTextStream("localhost", 9999)
      .map(line => {
        val fields = line.split(" ")
        (fields(0), fields(1).toLong)
      })

    val waterDS = socketStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          var currentMaxNum = 0L
          val time = new DateTime();

          override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
            //抽取字段中的时间戳
            val eTime = element._2
            //获取当前的最大时间戳
            currentMaxNum = Math.max(currentMaxNum, eTime)
            //延迟时间设置为2s
            val watermark = currentMaxNum - 2000
            println(
              "数据:" + element.toString() + ","
                + time.withMillis(eTime).toString("yyyy-MM-dd hh:mm:ss")
                + ",当前watermark:"
                + time.withMillis(watermark).toString("yyyy-MM-dd hh:mm:ss")
            )
            eTime
          }
        })
    )

    waterDS.keyBy(_._1).timeWindow(Time.seconds(3))
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print()

    env.execute()


  }
}
