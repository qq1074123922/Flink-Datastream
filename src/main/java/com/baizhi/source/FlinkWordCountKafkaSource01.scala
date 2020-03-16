package com.baizhi.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkWordCountKafkaSource01 {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化
    val props = new Properties()
    props.setProperty("bootstrap.servers", "CentOS:9092")
    props.setProperty("group.id", "g1")
    val text = env.addSource(new FlinkKafkaConsumer[String]("topic01",new SimpleStringSchema(),props))
    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .sum(1)

    //4.将计算的结果在控制打印
    counts.print()

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
