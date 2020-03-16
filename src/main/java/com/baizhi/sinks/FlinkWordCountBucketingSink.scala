package com.baizhi.sinks

import org.apache.flink.api.common.serialization.{Encoder, SimpleStringEncoder}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

object FlinkWordCountBucketingSink {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //2.创建DataStream - 细化
    val text = env.readTextFile("hdfs://CentOS:9000/demo/words")

    var bucketingSink=StreamingFileSink.forRowFormat(new Path("hdfs://CentOS:9000/bucket-results"),
      new SimpleStringEncoder[(String,Int)]("UTF-8")) // 所有数据都写到同一个路径
      .withBucketAssigner(new DateTimeBucketAssigner[(String, Int)]("yyyy-MM-dd"))
      .build()

    //3.执行DataStream的转换算子
    val counts = text.flatMap(line=>line.split("\\s+"))
      .map(word=>(word,1))
      .keyBy(0)
      .sum(1)

    counts.addSink(bucketingSink)

    //5.执行流计算任务
    env.execute("Window Stream WordCount")

  }
}
