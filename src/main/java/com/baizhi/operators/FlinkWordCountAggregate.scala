package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

object FlinkWordCountAggregate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("CentOS", 9999)

    lines.flatMap(_.split("\\s+"))
        .map((_,1))
        .keyBy("_1")
        .sum("_2")
        .print()

    env.execute("Stream WordCount")

  }
}
