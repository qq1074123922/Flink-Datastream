package com.baizhi.queryable

import java.util.function.Consumer

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.queryablestate.client.QueryableStateClient

object FlinkWordCountQueryClient01 {
  def main(args: Array[String]): Unit = {

    val client = new QueryableStateClient("CentOS", 9069)
    var jobID=JobID.fromHexString("dc60cd61dc2d591014c062397e3bd6b9")
    var queryName="wordcount"
    var queryKey="this"

    var rsd=new ReducingStateDescriptor[(String,Int)]("xxxx",new ReduceFunction[(String, Int)] {
      override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
        (v1._1,(v1._2+v2._2))
      }
    },createTypeInformation[(String,Int)])

    val resultFuture = client.getKvState(jobID, queryName, queryKey, createTypeInformation[String], rsd)

       val state: ReducingState[(String, Int)] = resultFuture.get()
       println("结果："+state.get())
//    resultFuture.thenAccept(new Consumer[ReducingState[(String, Int)]] {
//      override def accept(t: ReducingState[(String, Int)]): Unit = {
//        println("结果："+t.get())
//       }
//    })


   // Thread.sleep(10000)
    client.shutdownAndWait()
  }
}
