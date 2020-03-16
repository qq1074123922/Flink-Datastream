package com.baizhi.join

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object FlinkSessionWindowJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //001 zhansgan 时间戳
    val stream1 = env.socketTextStream("CentOS", 9999)
      .map(_.split("\\s+"))
      .map(ts=>(ts(0),ts(1),ts(2).toLong))
      .assignTimestampsAndWatermarks(new SessionAssignerWithPeriodicWatermarks)
    //apple 001 时间戳
    val stream2 = env.socketTextStream("CentOS", 8888)
                     .map(_.split("\\s+"))
                     .map(ts=>(ts(0),ts(1),ts(2).toLong))
                     .assignTimestampsAndWatermarks(new SessionAssignerWithPeriodicWatermarks)

    stream1.join(stream2)
            .where(t=>t._1) //stream1 用户ID
            .equalTo(t=> t._2) //stream2 用户ID
            .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
            .apply((s1,s2,out:Collector[(String,String,String)])=>{
              out.collect(s1._1,s1._2,s2._1)
            })
           .print("join结果")

    env.execute("FlinkSessionWindowJoin")
  }

}
class SessionAssignerWithPeriodicWatermarks  extends AssignerWithPeriodicWatermarks[(String,String,Long)] {

  var maxAllowOrderness=2000L
  var maxSeenEventTime= 0L //不可以取Long.MinValue

  var sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //系统定期的调用 计算当前的水位线的值
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxSeenEventTime-maxAllowOrderness)
  }

  //更新水位线的值，同时提取EventTime
  override def extractTimestamp(element: (String,String, Long), previousElementTimestamp: Long): Long = {
    //始终将最大的时间返回
    maxSeenEventTime=Math.max(maxSeenEventTime,element._3)
    println("ET:"+(element._1,element._2,sdf.format(element._3))+" WM:"+sdf.format(maxSeenEventTime-maxAllowOrderness))
    element._3
  }
}
