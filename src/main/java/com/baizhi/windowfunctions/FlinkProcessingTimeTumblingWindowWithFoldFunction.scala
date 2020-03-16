package com.baizhi.windowfunctions

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkProcessingTimeTumblingWindowWithFoldFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      //.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .fold(("",0),new UserDefineFoldFunction)
      .print()

    //5.执行流计算任务
    env.execute("Tumbling Window Stream WordCount")
  }
}
class UserDefineFoldFunction extends FoldFunction[(String,Int),(String,Int)]{
  override def fold(accumulator: (String, Int), value: (String, Int)): (String, Int) = {
    println("fold:"+accumulator+"\t"+value)
    (value._1,accumulator._2+value._2)
  }
}
