package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkWordCountValueState {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .map(new WordCountMapFunction)

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
