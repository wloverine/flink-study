package com.jkl.scala.windowtest

import com.jkl.scala.sourcetest.MySensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 每间隔一定时间，统计一次最高温度
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.addSource(new MySensorSource())
      .keyBy(_.id)
      //      .timeWindow(Time.minutes(1),Time.seconds(10))
      .window(
        TumblingProcessingTimeWindows.of(
          //第一个参数是每多少时间统计一次。第二个参数是偏移量。
          //此处就代表每分钟的第20s会计算一次窗口。注意如果想要每天统计一次的话，需要这么写:
          //Time.days(1),Time.hours(-8)。因为中国时区是utc+8，而此函数用的是格林威治时间，需要往前挪8小时。
          Time.minutes(1),Time.seconds(20)
        )
      )
      .maxBy(2)
      .print()

    env.execute("WindowTest")
  }
}
