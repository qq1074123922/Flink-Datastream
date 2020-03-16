package com.baizhi.windowfunctions

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkProcessingTimeSessionWindowWithWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(t=>t._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .apply(new UserDefineWindowFunction)
      .print()

    //5.执行流计算任务
    env.execute("Session Window Stream WordCount")
  }
}
class UserDefineWindowFunction extends WindowFunction[(String,Int),(String,Int),String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[(String, Int)],
                     out: Collector[(String, Int)]): Unit = {

    val sdf = new SimpleDateFormat("HH:mm:ss")
    var start=sdf.format(window.getStart)
    var end=sdf.format(window.getEnd)
    var sum = input.map(_._2).sum
    out.collect((s"${key}\t${start}~${end}",sum))

  }
}
