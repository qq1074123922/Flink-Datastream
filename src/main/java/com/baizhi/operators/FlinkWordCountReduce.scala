package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

object FlinkWordCountReduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("CentOS", 9999)

    lines.flatMap(_.split("\\s+"))
        .map((_,1))
        .keyBy("_1")
        .reduce((v1,v2)=>(v1._1,v1._2+v2._2))
        .print()

    env.execute("Stream WordCount")

  }
}
