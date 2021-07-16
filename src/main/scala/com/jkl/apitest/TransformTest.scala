package com.jkl.apitest

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //minBy
    val stream1 = env.readTextFile("C:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
      .minBy(2)

    //reduce
    val stream2 = env.readTextFile("C:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature))

    //split&select
    val splitStream = stream2.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    //Connect&CoMap
    val warning: DataStream[(String, Double)] = high.map(sensorData => {
      (sensorData.id, sensorData.temperature)
    })
    //connect连接的两个数据流只是被放在了同一个流中，内部依然保持各自数据和形式不变，两个流互相独立。
    val connected: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    //作用于ConnectedStreams上，功能与map和flatMap一样，对ConnectedStreams中的每一个Stream分别进行map和flatMap处理。
    val coMap: DataStream[Product] = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    //1.Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap 中再去调整成为一样的。
    //2.Connect 只能操作两个流，Union 可以操作多个。
    val union = high.union(low)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()
    union.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

    env.execute("TransformTest")
  }
}
