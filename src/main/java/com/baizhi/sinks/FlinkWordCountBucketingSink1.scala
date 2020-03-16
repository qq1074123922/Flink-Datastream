package com.baizhi.sinks

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FlinkWordCountBucketingSink1 {
  def main(args: Array[String]): Unit = {
    //1.创建流计算执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //2.创建DataStream - 细化
    val text = env.readTextFile("hdfs://CentOS:9000/demo/words")

    var bucketingSink=new BucketingSink[(String,Int)]("hdfs://CentOS:9000/bucket-results")
    bucketingSink.setBucketer(new DateTimeBucketer[(String,Int)]("yyyy-MM-dd"))
    bucketingSink.setBatchSize(1024)

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
