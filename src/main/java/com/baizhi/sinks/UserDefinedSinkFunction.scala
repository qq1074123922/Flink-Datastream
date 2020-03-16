package com.baizhi.sinks

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class UserDefinedSinkFunction  extends RichSinkFunction[(String,Int)]{

  override def open(parameters: Configuration): Unit = {
    println("打开链接...")
  }

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    println("输出："+value)
  }

  override def close(): Unit = {
    println("释放连接")
  }
}
