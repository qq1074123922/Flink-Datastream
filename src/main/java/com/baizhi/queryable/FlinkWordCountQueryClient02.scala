package com.baizhi.queryable

import java.util.function.Consumer

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.scala.createTypeInformation

object FlinkWordCountQueryClient02 {
  def main(args: Array[String]): Unit = {

    val client = new QueryableStateClient("CentOS", 9069)
    val vsd = new ValueStateDescriptor[Int]("xxx", createTypeInformation[Int])
    var jobID=JobID.fromHexString("c6c40ac98d5a85ef2b24a609f046053e")

    val resultFuture = client.getKvState(jobID,"query-wc","this",
      createTypeInformation[String],vsd)

    val result = resultFuture.get()
    val count = result.value()

    println("count:"+count)
    client.shutdownAndWait()
  }
}
