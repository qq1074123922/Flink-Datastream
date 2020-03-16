package com.baizhi.operatorstate

import java.util

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import java.lang.{Long => JLong}
import java.util.Collections
import scala.collection.JavaConverters._
class UserDefineCounterSource  extends RichParallelSourceFunction[Long] with ListCheckpointed[JLong]{
  @volatile
  private var isRunning = true
  private var offset = 0L

  //存储状态值
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[JLong] = {
    println("snapshotState:"+offset)
    Collections.singletonList(offset)//返回一个不可拆分集合
  }

  override def restoreState(state: util.List[JLong]): Unit = {
     println("restoreState:"+state.asScala)
     offset=state.asScala.head //取第一个元素
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    val lock = ctx.getCheckpointLock
    while (isRunning) {
      Thread.sleep(1000)
      lock.synchronized({
        ctx.collect(offset) //往下游输出当前offset
        offset += 1
      })
    }
  }

  override def cancel(): Unit = isRunning=false
}
