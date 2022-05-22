package com.jkl.scala.transformtest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPutTest {
  case class mdmsg(mdType: String, url: String, time: Long)

  val webTerminal = new OutputTag[mdmsg]("web端埋点")
  val mobileTerminal = new OutputTag[mdmsg]("移动端埋点")
  val csTerminal = new OutputTag[mdmsg]("cs端埋点")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //输入流
    //web,http://www.web1.com,1587787201000
    //mobile,http://www.mobile1.com,1587787202000
    //cs,http://www.cs1.com,1587787203000
    val socketData = env.socketTextStream("localhost", 9999)
    socketData.print("input data")

    val outputStream: DataStream[mdmsg] = socketData.map(line => {
      val fields = line.split(",")
      mdmsg(fields(0), fields(1), fields(2).toLong)
    })
      .process((value: mdmsg, ctx: ProcessFunction[mdmsg, mdmsg]#Context, out: Collector[mdmsg]) => {
        if (value.mdType == "web") {
          ctx.output(webTerminal, value)
        }
        else if (value.mdType == "mobile") {
          ctx.output(mobileTerminal, value)
        }
        else if (value.mdType == "cs") {
          ctx.output(csTerminal, value)
        }
        else {
          out.collect(value)
        }
      })

    outputStream.getSideOutput(webTerminal).print("web")
    outputStream.getSideOutput(mobileTerminal).print("mobile")
    outputStream.getSideOutput(csTerminal).print("cs")

    env.execute("SideOutPutTest")

  }
}
