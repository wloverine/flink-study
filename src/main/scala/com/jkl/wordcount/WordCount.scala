package com.jkl.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCount extends App {
  //创建执行环境
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val tool: ParameterTool = ParameterTool.fromArgs(args)

  val file: String = tool.get("file")

  //读取文本文件
  env.readTextFile(file)
    .flatMap(_.split(" "))
    .map((_, 1))
    .groupBy(0)
    .sum(1)
    .print()
}
