package com.jkl.scala.transformtest

import com.jkl.scala.sourcetest.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path: String = this.getClass.getClassLoader.getResource("sensor.txt").getPath

    env.setParallelism(1)

    //minBy
    //min和minBy的区别是min除了会将分组字段（此处为id）以及比较字段返回，其他字段则保留其第一次出现时候的值。
    //而minBy则是保留比较字段最小的对应的整个元素。
    val stream1: DataStream[SensorReading] = env.readTextFile(path)
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
      .minBy(2) //实时输出每个key对应温度最小的那条记录

    //reduce
    val stream2: DataStream[SensorReading] = env.readTextFile(path)
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature))

//    //split&select
//    val splitStream: SplitStream[SensorReading] = stream2.split(data => {
//      if (data.temperature > 30) Seq("high") else Seq("low")
//    })
//
//    val high: DataStream[SensorReading] = splitStream.select("high")
//    val low: DataStream[SensorReading] = splitStream.select("low")
//    val all: DataStream[SensorReading] = splitStream.select("high", "low")
//
//    //Connect&CoMap
//    val warning: DataStream[(String, Double)] = high.map(sensorData => {
//      (sensorData.id, sensorData.temperature)
//    })
//    //connect连接的两个数据流只是被放在了同一个流中，内部依然保持各自数据和形式不变，两个流互相独立。
//    val connected: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)
//
//    //作用于ConnectedStreams上，功能与map和flatMap一样，对ConnectedStreams中的每一个Stream分别进行map和flatMap处理。
//    val coMap: DataStream[Product] = connected.map(
//      warningData => (warningData._1, warningData._2, "warning"),
//      lowData => (lowData.id, "healthy")
//    )
//
//    //1.Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap 中再去调整成为一样的。
//    //2.Connect 只能操作两个流，Union 可以操作多个。
//    val union = high.union(low)
//
//    stream1.print()

    env.execute("TransformTest")
  }
}
