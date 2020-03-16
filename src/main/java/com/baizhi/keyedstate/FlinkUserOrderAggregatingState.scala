package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkUserOrderAggregatingState {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化 001 zhangsan 1000
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.map(line=>line.split("\\s+"))
      .map(ts=>(ts(0)+":"+ts(1),ts(2).toDouble))
      .keyBy(0)
      .map(new UserOrderAggregatingStateMapFunction)

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Stream WordCount")
  }
}
class UserOrderAggregatingStateMapFunction extends RichMapFunction[(String,Double),(String,Double)]{
    var as:AggregatingState[Double,Double]=_


    override def open(parameters: Configuration): Unit = {
    //1.创建对应状态描述符
    val asd = new AggregatingStateDescriptor[Double,(Int,Double),Double]("userOrderAggregatingStateMapFunction",
      new AggregateFunction[Double,(Int,Double),Double](){
        override def createAccumulator(): (Int, Double) = (0,0.0)

        override def add(value: Double, accumulator: (Int, Double)): (Int, Double) = {
          (accumulator._1+1,accumulator._2+value)
        }

        override def getResult(accumulator: (Int, Double)): Double = {
          accumulator._2/accumulator._1
        }

        override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = {
          (a._1+b._1,a._2+b._2)
        }
      },createTypeInformation[(Int,Double)])

    //2.获取RuntimeContext
    var context: RuntimeContext = getRuntimeContext
    //3.获取指定类型状态
    as=context.getAggregatingState(asd)
  }

  override def map(value: (String, Double)): (String, Double) = {
     as.add(value._2)
    //返回最新值
    (value._1,as.get())
  }
}
