package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    env.setParallelism(1)

    val dataStream = env.readTextFile(path)
      .map(data => {
        val fields = data.split(",")
        SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
      })

    val sinkFunction: SinkFunction[SensorReading] = JdbcSink.sink(
      "insert into sensor_temperature(id,timestamp,temperature) values(?,?,?)",
      (ps, sensor) => {
        ps.setObject(1, sensor.id)
        ps.setObject(2, sensor.timestamp)
        ps.setObject(3, sensor.temperature)
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(5)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/learnjdbc")
        .withDriverName("org.mariadb.jdbc.Driver")
        .withUsername("root")
        .withPassword("jkl123")
        .build()
    )

    dataStream.addSink(sinkFunction)

    env.execute("JDBCSinkTest")
  }
}



