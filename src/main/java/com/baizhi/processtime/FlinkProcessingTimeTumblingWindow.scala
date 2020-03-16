package com.baizhi.processtime

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkProcessingTimeTumblingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((v1,v2)=>(v1._1,v1._2+v2._2))
      .print()

    //5.执行流计算任务
    env.execute("Tumbling Window Stream WordCount")
  }
}
