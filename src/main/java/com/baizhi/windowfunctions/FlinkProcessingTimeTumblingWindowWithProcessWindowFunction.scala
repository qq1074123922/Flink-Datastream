package com.baizhi.windowfunctions

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkProcessingTimeTumblingWindowWithProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(t=>t._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new UserDefineProcessWindowFunction)
      .print()

    //5.执行流计算任务
    env.execute("Tumbling Window Stream WordCount")
  }
}
class UserDefineProcessWindowFunction extends ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow]{
  val sdf=new SimpleDateFormat("HH:mm:ss")
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Int)],
                       out: Collector[(String, Int)]): Unit = {
    val w = context.window//获取窗口元数据
    val start =sdf.format(w.getStart)
    val end = sdf.format(w.getEnd)

    val list = elements.toList
    println("list:"+list)

    val total=list.map(_._2).sum
    out.collect((key+"\t["+start+"~"+end+"]",total))
  }
}