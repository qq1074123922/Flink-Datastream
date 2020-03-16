package com.baizhi.windowfunctions

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkProcessingTimeTumblingWindowWithReduceFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce(new UserDefineReduceFunction)
      .print()

    //5.执行流计算任务
    env.execute("Tumbling Window Stream WordCount")
  }
}
class UserDefineReduceFunction extends ReduceFunction[(String,Int)]{
  override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
    println("reduce:"+v1+"\t"+v2)
    (v1._1,v2._2+v1._2)
  }
}
