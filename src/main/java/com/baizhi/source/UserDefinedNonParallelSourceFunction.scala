package com.baizhi.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class UserDefinedNonParallelSourceFunction extends SourceFunction[String]{
  @volatile //防止线程拷贝变量
  var isRunning:Boolean=true
  val lines:Array[String]=Array("this is a demo","hello world","ni hao ma")

  //在该方法中启动线程，通过sourceContext的collect方法发送数据
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while(isRunning){
      println("Thread："+Thread.currentThread().getId)
      Thread.sleep(5000)

      //输送数据给下游
      sourceContext.collect(lines(new Random().nextInt(lines.size)))
    }
  }
  //释放资源
  override def cancel(): Unit = {
    isRunning=false
  }
}
