package com.baizhi.sinks

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.kafka.clients.producer.ProducerConfig

object FlinkWordCountKafkaSink02 {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //2.创建DataStream - 细化
    val text = env.readTextFile("hdfs://CentOS:9000/demo/words")

    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092")
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"100")
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"500")

    //Semantic.EXACTLY_ONCE:开启kafka幂等写特性
    //Semantic.AT_LEAST_ONCE:开启Kafka Retries机制
    val kafakaSink = new FlinkKafkaProducer[(String, Int)]("defult_topic",
      new UserDefinedKeyedSerializationSchema, props, Semantic.AT_LEAST_ONCE)
    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .sum(1)

    counts.addSink(kafakaSink)

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
