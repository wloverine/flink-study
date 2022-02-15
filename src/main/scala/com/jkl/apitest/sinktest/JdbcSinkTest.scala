package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

/**
 * 将sensor.txt中的数据读取拆分字段后写入mysql
 */
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

    val sql = "insert into sensor_temperature(id,timestamp,temperature) values(?,?,?)"

    val statementBuilder: JdbcStatementBuilder[SensorReading] = new JdbcStatementBuilder[SensorReading] {
      override def accept(t: PreparedStatement, u: SensorReading): Unit = {
        t.setObject(1, u.id)
        t.setObject(2, u.timestamp)
        t.setObject(3, u.temperature)
      }
    }

    val sinkFunction: SinkFunction[SensorReading] = JdbcSink.sink(
      sql,
      statementBuilder,
      JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(5)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/spark")
        .withDriverName("org.mariadb.jdbc.Driver")
        .withUsername("root")
        .withPassword("jkl123")
        .build()
    )

    dataStream.addSink(sinkFunction)

    env.execute("JDBCSinkTest")
  }
}



