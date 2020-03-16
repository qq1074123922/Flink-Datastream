package com.baizhi.queryable

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.QueryableStateStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object FlinkWordCountQueryableStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //间隔5s执行一次checkpoint 精准一次
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    //设置检查点超时 4s
    env.getCheckpointConfig.setCheckpointTimeout(4000)
    //开启本次检查点 与上一次完成的检查点时间间隔不得小于 2s 优先级高于 checkpoint interval
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)
    //如果检查点失败，任务宣告退出 setFailOnCheckpointingErrors(true)
   // env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    //设置如果任务取消，系统该如何处理检查点数据
    //RETAIN_ON_CANCELLATION:如果取消任务的时候，没有加--savepoint，系统会保留检查点数据
    //DELETE_ON_CANCELLATION:取消任务，自动是删除检查点（不建议使用）
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    var rsd=new ReducingStateDescriptor[(String,Int)]("reducestate",new ReduceFunction[(String, Int)] {
      override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
        (v1._1,(v1._2+v2._2))
      }
    },createTypeInformation[(String,Int)])

   env.socketTextStream("CentOS", 9999)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .keyBy(0)
      .asQueryableState("wordcount", rsd)

    //5.执行流计算任务
    env.execute("Stream WordCount")

  }
}
