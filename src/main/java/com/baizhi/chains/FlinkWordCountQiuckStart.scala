package com.baizhi.chains

import org.apache.flink.streaming.api.scala._

object FlinkWordCountQiuckStart {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.disableOperatorChaining()
    val text = env.socketTextStream("CentOS", 9999)
                  .slotSharingGroup("g1")
                  .flatMap(line=>line.split("\\s+"))
                  .map(word=>(word,1))
                  .print()
                  .slotSharingGroup("g2")

    println(env.getExecutionPlan)
    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
