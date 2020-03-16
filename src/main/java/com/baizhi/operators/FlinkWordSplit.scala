package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

object FlinkWordSplit {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text1 = env.socketTextStream("CentOS", 9999)

    var splitStream= text1.split(line=> {
         if(line.contains("error")){
           List("error")
         } else{
           List("info")
         }
     })
    splitStream.select("error").printToErr("错误")
    splitStream.select("info").print("信息")
    splitStream.select("error","info").print("All")

    env.execute("Stream WordCount")

  }
}
