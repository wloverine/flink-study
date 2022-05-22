package com.jkl.scala.windowtest

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.DateTime

import java.time.Duration

/**
 * 接收socket的数据，并设置最大延迟时间为2s，窗口时间为3s
数据长这样：
s3 1639100010955
s2 1639100009955
s1 1639100008955
s0 1639100007955
s4 1639100011955
s5 1639100012955
s6 1639100013955
s7 1639100016955
 */
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
    
    //分组聚合，根据key相同的来进行分组，并且设置窗口大小为3s
    waterDS.keyBy(_._1).timeWindow(Time.seconds(3))
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print()

    env.execute()


  }
}
