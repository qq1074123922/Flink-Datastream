package com.baizhi.trigger

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object FlinkTumblingWindowWithCountTrigger {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(new UserDefineCountTrigger(4L))
      .apply(new UserDefineGlobalWindowFunction)
      .print()

    //5.执行流计算任务
    env.execute("Global Window Stream WordCount")
  }
}
class UserDefineGlobalWindowFunction extends AllWindowFunction[String,String,TimeWindow]{

  override def apply(window: TimeWindow,
                     input: Iterable[String],
                     out: Collector[String]): Unit = {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    var start=sdf.format(window.getStart)
    var end=sdf.format(window.getEnd)
    var windowContent=input.toList
    println("window:"+start+"\t"+end+" "+windowContent.mkString(" | "))
  }
}
