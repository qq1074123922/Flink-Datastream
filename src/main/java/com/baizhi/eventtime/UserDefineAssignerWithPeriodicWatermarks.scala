package com.baizhi.eventtime

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class UserDefineAssignerWithPeriodicWatermarks  extends AssignerWithPeriodicWatermarks[(String,Long)] {

  var maxAllowOrderness=2000L
  var maxSeenEventTime= 0L //不可以取Long.MinValue

  var sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //系统定期的调用 计算当前的水位线的值
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxSeenEventTime-maxAllowOrderness)
  }

  //更新水位线的值，同时提取EventTime
  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    //始终将最大的时间返回
    maxSeenEventTime=Math.max(maxSeenEventTime,element._2)
    println("ET:"+(element._1,sdf.format(element._2))+" WM:"+sdf.format(maxSeenEventTime-maxAllowOrderness))
    element._2
  }
}
