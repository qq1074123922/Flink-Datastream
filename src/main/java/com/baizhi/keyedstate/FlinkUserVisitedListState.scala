package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

object FlinkUserVisitedListState {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化 001 zhangsan 电子类 xxxx  001 zhangsan 手机类 xxxx 001 zhangsan 母婴类 xxxx
    val text = env.socketTextStream("CentOS", 9999)

    //3.执行DataStream的转换算子
    val counts = text.map(line=>line.split("\\s+"))
      .map(ts=>(ts(0)+":"+ts(1),ts(2)))
      .keyBy(0)
      .map(new UserVisitedMapFunction)

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Stream WordCount")
  }
}
class UserVisitedMapFunction extends RichMapFunction[(String,String),(String,String)]{
  var userVisited:ListState[String]=_


  override def open(parameters: Configuration): Unit = {
    //1.创建对应状态描述符
    val lsd = new ListStateDescriptor[String]("userVisited", createTypeInformation[String])
    //2.获取RuntimeContext
    var context: RuntimeContext = getRuntimeContext
    //3.获取指定类型状态
    userVisited=context.getListState(lsd)
  }

  override def map(value: (String, String)): (String, String) = {
    //获取历史值
    var historyData = userVisited.get().asScala.toList
    //更新状态
    historyData = historyData.::(value._2).distinct
    userVisited.update(historyData.asJava)

    //返回最新值
    (value._1,historyData.mkString(" | "))
  }
}
