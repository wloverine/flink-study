package com.jkl.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //用flink自带的ParameterTool静态类来获取外部传来的参数
  val tool: ParameterTool = ParameterTool.fromArgs(args)
  val host = tool.get("host")
  val port = tool.getInt("port")

  env.socketTextStream(host, port)
    .flatMap(_.split(" "))
    .map((_, 1))
    .keyBy(0)
    .sum(1)
    .print().setParallelism(1)

  env.execute("stream word count")


}
