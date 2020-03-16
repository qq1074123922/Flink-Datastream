package com.baizhi.sinks

import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

object FlinkWordCountFileSink {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化
    val text = env.readTextFile("hdfs://CentOS:9000/demo/words")

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .sum(1)

    //4.将计算的结果在控制打印
    counts.writeAsText("hdfs://CentOS:9000/flink-results",WriteMode.OVERWRITE)

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
