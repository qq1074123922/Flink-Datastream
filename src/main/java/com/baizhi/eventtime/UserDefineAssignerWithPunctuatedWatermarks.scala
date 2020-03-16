package com.baizhi.eventtime

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

class UserDefineAssignerWithPunctuatedWatermarks extends AssignerWithPunctuatedWatermarks[(String,Long)] {

  var maxAllowOrderness=2000L
  var maxSeenEventTime= Long.MinValue

  var sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //每接收一条记录系统计算一次
  override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
    maxSeenEventTime=Math.max(maxSeenEventTime,lastElement._2)
    println("ET:"+(lastElement._1,sdf.format(lastElement._2))+" WM:"+sdf.format(maxSeenEventTime-maxAllowOrderness))
     new Watermark(maxSeenEventTime-maxAllowOrderness)
  }

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    //始终将最大的时间返回
    element._2
  }
}
