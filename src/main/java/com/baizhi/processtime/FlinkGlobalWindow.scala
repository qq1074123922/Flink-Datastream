package com.baizhi.processtime

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object FlinkGlobalWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(t=>t._1)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(4))
      .apply(new UserDefineGlobalWindowFunction)
      .print()

    //5.执行流计算任务
    env.execute("Global Window Stream WordCount")
  }
}
class UserDefineGlobalWindowFunction extends WindowFunction[(String,Int),(String,Int),String,GlobalWindow]{
  override def apply(key: String,
                     window: GlobalWindow,
                     input: Iterable[(String, Int)],
                     out: Collector[(String, Int)]): Unit = {
    var sum = input.map(_._2).sum
    out.collect((s"${key}",sum))
  }
}
