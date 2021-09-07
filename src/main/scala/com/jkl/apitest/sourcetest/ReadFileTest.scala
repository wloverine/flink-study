package com.jkl.apitest.sourcetest

import org.apache.flink.api.common.io.GlobFilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

import java.util
import java.util.Collections

object ReadFileTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //指定输入路径
    val path = "./src/main/resources/test_read"

    val format = new TextInputFormat(new Path(path))
    //设置文件过滤，排除掉1.txt
    format.setFilesFilter(
      new GlobFilePathFilter(
        Collections.singletonList("**"), util.Arrays.asList("**/1.txt")
      )
    )

    env.readFile(format, path)
      .writeAsText("./tmp/out")

    env.execute("test_read")


  }
}
