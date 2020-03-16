package com.baizhi.source

import org.apache.flink.streaming.api.scala._

object FlinkWordCountCollection {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化
    val text = env.fromCollection(List("this is a demo","hello word"))

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .sum(1)

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
