package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, MapFunction, ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, FoldingState, FoldingStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction

object FlinkUserOrderAggregatingState02 {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化 001 zhangsan 1000
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.map(line=>line.split("\\s+"))
      .map(ts=>(ts(0)+":"+ts(1),ts(2).toDouble))
      .keyBy(0)
      .map(new UserOrderAvgMapFunction)

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Stream WordCount")
  }
}
class UserOrderAvgMapFunction extends RichMapFunction[(String,Double),(String,Double)]{
    var rs:ReducingState[Int]=_
    var fs:FoldingState[Double,Double]=_

    override def open(parameters: Configuration): Unit = {
      //1.创建对应状态描述符
      val rsd = new ReducingStateDescriptor[Int]("wordcountReducingStateDescriptor",
        new ReduceFunction[Int](){
          override def reduce(v1: Int, v2: Int): Int = v1+v2
        },createTypeInformation[Int])

      val fsd=new FoldingStateDescriptor[Double,Double]("foldstate",0,new FoldFunction[Double,Double](){
        override def fold(accumulator: Double, value: Double): Double = {
          accumulator+value
        }
      },createTypeInformation[Double])

      //2.获取RuntimeContext
      var context: RuntimeContext = getRuntimeContext
      //3.获取指定类型状态
      rs=context.getReducingState(rsd)
      fs=context.getFoldingState(fsd)
    }

  override def map(value: (String, Double)): (String, Double) = {
    rs.add(1)
    fs.add(value._2)
    //返回最新值
    (value._1,fs.get()/rs.get())
  }
}
