package com.jkl.scala.sourcetest

import org.apache.flink.streaming.api.scala._

object IterationTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val someIntegers = env.generateSequence(0, 1000)

    val iteratedStream = someIntegers.iterate(
      iteration => {
        val minusOne = iteration.map(v => v - 1) //迭代体，在对流里的元素进行map转换后满足第一个filter条件的会继续迭代
        val stillGreaterThanZero = minusOne.filter(_ > 10) //大于10的元素会被送回feedback通道继续迭代，其余元素向下游转发
        val lessThanZero = minusOne.filter(_ <= 10) //小于等于10的元素最终会被输出
        (stillGreaterThanZero, lessThanZero)
      }
    )

    iteratedStream.print()


    env.execute("IterationTest")


  }
}
