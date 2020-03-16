package com.baizhi.eventtime

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UserDefineAllWindowFucntion extends AllWindowFunction[(String,Long),(String),TimeWindow]{
  var sdf=new SimpleDateFormat("HH:mm:ss")
  override def apply(window: TimeWindow,
                     input: Iterable[(String, Long)],
                     out: Collector[String]): Unit = {

    var start=sdf.format(window.getStart)
    var end=sdf.format(window.getEnd)
    // 起始~终止\t 元素:时间 | ...
    out.collect(start+" ~ "+end +" \t"+input.map(t=>t._1+":"+sdf.format(t._2)).mkString(" | "))
  }
}