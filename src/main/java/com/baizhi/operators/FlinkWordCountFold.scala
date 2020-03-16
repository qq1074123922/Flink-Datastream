package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

object FlinkWordCountFold {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("CentOS", 9999)

    lines.flatMap(_.split("\\s+"))
        .map((_,1))
        .keyBy("_1")
        .fold((null:String,0:Int))((z,v)=>(v._1,v._2+z._2))
        .print()

    env.execute("Stream WordCount")

  }
}
