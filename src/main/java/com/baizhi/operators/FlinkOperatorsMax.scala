package com.baizhi.operators

import org.apache.flink.streaming.api.scala._

case class Emp(name:String,dept:String,salary:Double)
object FlinkOperatorsMax {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //zhangsan 研发部 1000
    //lisi 研发部 ww 销售部 90005000
    //ww 销售部 9000
    val lines = env.socketTextStream("CentOS", 9999)

    lines.map(line=>line.split(" "))
        .map(ts=>Emp(ts(0),ts(1),ts(2).toDouble))
        .keyBy("dept")
        .maxBy("salary")//Emp(lisi,研发部,5000.0)
        .print()

    env.execute("Stream WordCount")

  }
}
