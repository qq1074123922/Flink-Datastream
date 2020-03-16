package com.baizhi.checkpoints

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object FlinkWordCountValueStateCheckpoint {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("hdfs:///flink-rocksdb-checkpoints",true))

    //间隔5s执行一次checkpoint 精准一次
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    //设置检查点超时 4s
    env.getCheckpointConfig.setCheckpointTimeout(4000)
    //开启本次检查点 与上一次完成的检查点时间间隔不得小于 2s 优先级高于 checkpoint interval
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)
    //如果检查点失败，任务宣告退出 setFailOnCheckpointingErrors(true)
    //env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    //设置如果任务取消，系统该如何处理检查点数据
    //RETAIN_ON_CANCELLATION:如果取消任务的时候，没有加--savepoint，系统会保留检查点数据
    //DELETE_ON_CANCELLATION:取消任务，自动是删除检查点（不建议使用）
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //2.创建DataStream - 细化
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
                      .map(word=>(word,1))
                      .keyBy(0)
                      .map(new WordCountMapFunction)
                      .uid("map-wc")

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Stream WordCount")
  }
}
class WordCountMapFunction extends RichMapFunction[(String,Int),(String,Int)]{
  var vs:ValueState[Int]=_


  override def open(parameters: Configuration): Unit = {
    //1.创建对应状态描述符
    val vsd = new ValueStateDescriptor[Int]("wordcount", createTypeInformation[Int])
    //2.获取RuntimeContext
    var context: RuntimeContext = getRuntimeContext
    //3.获取指定类型状态
    vs=context.getState(vsd)
  }

  override def map(value: (String, Int)): (String, Int) = {
    //获取历史值
    val historyData = vs.value()
    //更新状态
    vs.update(historyData+value._2)
    //返回最新值
    (value._1,vs.value())
  }
}
