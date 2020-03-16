package com.baizhi.trigger
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.api.scala._
class UserDefineCountTrigger(maxCount:Long) extends Trigger[String,TimeWindow]{

  var rsd:ReducingStateDescriptor[Long]= new ReducingStateDescriptor[Long]("rsd",new ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = value1+value2
  },createTypeInformation[Long])

  override def onElement(element: String, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val state: ReducingState[Long] = ctx.getPartitionedState(rsd)
    state.add(1L)
    if(state.get() >= maxCount){
     state.clear()
     return TriggerResult.FIRE_AND_PURGE
    }else{
      return TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    println("==clear==")
    ctx.getPartitionedState(rsd).clear()
  }
}
