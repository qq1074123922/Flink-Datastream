package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

object FlinkWordCountConnectedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text1 = env.socketTextStream("CentOS", 9999)
    val text2 = env.socketTextStream("CentOS", 8888)

     text1.connect(text2)
          .flatMap((line:String)=>line.split("\\s+"),(line:String)=>line.split("\\s+"))
          .map((_,1))
          .keyBy(0)
          .sum(1)
          .print("总数")

    env.execute("Stream WordCount")

  }
}
