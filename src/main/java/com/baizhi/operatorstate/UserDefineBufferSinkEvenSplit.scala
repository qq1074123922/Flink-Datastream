package com.baizhi.operatorstate

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class UserDefineBufferSinkEvenSplit(threshold: Int = 0) extends SinkFunction[(String, Int)] with CheckpointedFunction{

  @transient
  private var checkpointedState: ListState[(String, Int)] = _

  private val bufferedElements = ListBuffer[(String, Int)]()

  //复写写出逻辑
  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    bufferedElements += value
    if(bufferedElements.size >= threshold){
      for(e <- bufferedElements){
        println("元素："+e)
      }
      bufferedElements.clear()
    }
  }

  //需要将状态数据存储起来
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
      println("Snapshot State.. ")
      checkpointedState.clear()
      checkpointedState.update(bufferedElements.asJava)//直接将状态数据存储起来
  }
  //初始化状态逻辑、状态恢复逻辑
  override def initializeState(context: FunctionInitializationContext): Unit = {
    println("initializeState")
    //初始化状态、也有可能是故障恢复
    val lsd=new ListStateDescriptor[(String, Int)]("list-state",createTypeInformation[(String,Int)])
    checkpointedState = context.getOperatorStateStore.getListState(lsd) //默认均分方式恢复
    if(context.isRestored){ //实现故障恢复逻辑
      bufferedElements.appendAll(checkpointedState.get().asScala.toList)
      println("State Restore:"+ bufferedElements.mkString(" , "))
    }
  }
}
