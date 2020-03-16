package com.baizhi.source

import java.util.Properties

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.htrace.shaded.fasterxml.jackson.databind.JsonNode

object FlinkWordCountKafkaSource03 {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.创建DataStream - 细化
    val props = new Properties()
    props.setProperty("bootstrap.servers", "CentOS:9092")
    props.setProperty("group.id", "g1")
    //{"id":1,"name":"zhangsan"}
    val text = env.addSource(new FlinkKafkaConsumer[ObjectNode]("topic01",new JSONKeyValueDeserializationSchema(true),props))
    //t:{"value":{"id":1,"name":"zhangsan"},"metadata":{"offset":0,"topic":"topic01","partition":13}}
   text.map(t=> (t.get("value").get("id").asInt(),t.get("value").get("name").asText()))
        .print()

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
