package com.baizhi.windowfunctions

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkProcessingTimeTumblingWindowWithAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(new UserDefineAggregateFunction)
      .print()

    //5.执行流计算任务
    env.execute("Tumbling Window Stream WordCount")
  }
}
class UserDefineAggregateFunction extends AggregateFunction[(String,Int),(String,Int),(String,Int)]{
  override def createAccumulator(): (String, Int) = ("",0)

  override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
    println("add:"+value+"\t"+accumulator)
    (value._1,value._2+accumulator._2)
  }

  override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

  override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
    println("merge:"+a+"\t"+b)
    (a._1,a._2+b._2)
  }
}
