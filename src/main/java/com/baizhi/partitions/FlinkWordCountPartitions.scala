package com.baizhi.partitions

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object FlinkWordCountPartitions {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

   env.socketTextStream("CentOS", 9999)
      .map((_,1))
      .partitionCustom(new Partitioner[String] {
        override def partition(key: String, numPartitions: Int): Int = {
          key.hashCode & Integer.MAX_VALUE % numPartitions
        }
      },_._1)
      .print()
      .setParallelism(4)
      println(env.getExecutionPlan)
      env.execute("Stream WordCount")

  }
}
