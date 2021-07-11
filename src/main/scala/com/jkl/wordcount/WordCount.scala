package com.jkl.wordcount

import org.apache.flink.api.scala._

object WordCount extends App {
  //创建执行环境
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  //读取文本文件
  env.readTextFile("/Users/daryl/IdeaProjects/flink-study/src/main/resources/test.txt")
    .flatMap(_.split(" "))
    .map((_, 1))
    .groupBy(0)
    .sum(1)
    .print()
}
