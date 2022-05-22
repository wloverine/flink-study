package com.jkl.scala.transformtest

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyByTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = this.getClass.getClassLoader.getResource("test.txt").getPath

    val stream: KeyedStream[(String, Int), String] = env.readTextFile(path)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
    
    stream.process(new KeyedProcessFunction[String, (String, Int), String] {
      override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), String]#Context, out: Collector[String]): Unit = {
        val rowKey = ctx.getCurrentKey
        val rowValue = value
        val output_value = s"key=${rowKey}###value=$rowValue" //定义新的输出行
        out.collect(output_value)
      }
    }).print()

    env.execute("KeyByTest")

  }
}
