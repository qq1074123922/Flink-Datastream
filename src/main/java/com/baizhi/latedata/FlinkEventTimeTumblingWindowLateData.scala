package com.baizhi.latedata

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkEventTimeTumblingWindowLateData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//方便测试将并行度设置为 1

    //默认时间特性是ProcessingTime，需要设置为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置定期调用水位线频次 1s
    // env.getConfig.setAutoWatermarkInterval(1000)

    //字符 时间戳
   env.socketTextStream("CentOS", 9999)
                  .map(line=>line.split("\\s+"))
                  .map(ts=>(ts(0),ts(1).toLong))
                  .assignTimestampsAndWatermarks(new UserDefineAssignerWithPunctuatedWatermarks)
                  .windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
                  .allowedLateness(Time.seconds(2))
                  .apply(new UserDefineAllWindowFucntion)
                  .print("输出")

    env.execute("Tumbling Event Time Window Stream")
  }
}

