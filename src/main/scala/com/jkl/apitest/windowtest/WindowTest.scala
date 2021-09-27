package com.jkl.apitest.windowtest

import com.jkl.apitest.sourcetest.MySensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 每间隔10s，统计一次最高温度
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.addSource(new MySensorSource())
      .keyBy(_.id)
//      .timeWindow(Time.minutes(1),Time.seconds(10))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .maxBy(2)
      .print()

    env.execute("WindowTest")
  }
}
