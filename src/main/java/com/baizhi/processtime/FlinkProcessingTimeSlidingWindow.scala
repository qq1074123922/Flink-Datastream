package com.baizhi.processtime

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkProcessingTimeSlidingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(4),Time.seconds(2)))
      .aggregate(new UserDefineAggregateFunction)
      .print()

    //5.执行流计算任务
    env.execute("Sliding Window Stream WordCount")
  }
}
class UserDefineAggregateFunction extends AggregateFunction[(String,Int),(String,Int),(String,Int)]{
  override def createAccumulator(): (String, Int) = ("",0)

  override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
    (value._1,value._2+accumulator._2)
  }

  override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

  override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
    (a._1,a._2+b._2)
  }
}