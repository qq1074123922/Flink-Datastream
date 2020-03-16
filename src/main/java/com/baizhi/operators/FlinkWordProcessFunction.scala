package com.baizhi.operators

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkWordProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("CentOS", 9999)

    val errorTag = new OutputTag[String]("error")
    val allTag = new OutputTag[String]("all")

    val infoStream = text.process(new ProcessFunction[String, String] {
      override def processElement(value: String,
                                  ctx: ProcessFunction[String, String]#Context,
                                  out: Collector[String]): Unit = {
        if (value.contains("error")) {
          ctx.output(errorTag, value) //边输出
        } else {
          out.collect(value) //正常数据
        }
        ctx.output(allTag, value) //边输出
      }
    })
    infoStream.getSideOutput(errorTag).printToErr("错误")
    infoStream.getSideOutput(allTag).printToErr("所有")
    infoStream.print("正常")

    env.execute("Stream WordCount")

  }
}
