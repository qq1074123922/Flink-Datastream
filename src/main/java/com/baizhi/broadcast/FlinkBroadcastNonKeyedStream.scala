package com.baizhi.broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//仅仅输出满足规则的数据
object FlinkBroadcastNonKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //吞吐量高
    val inputs = env.socketTextStream("CentOS", 9999)

    //定义需要广播流 吞吐量低
   val bcsd=new MapStateDescriptor[String,String]("bcsd",createTypeInformation[String],createTypeInformation[String])
   val broadcaststream = env.socketTextStream("CentOS", 8888)
                            .broadcast(bcsd)

    val tag = new OutputTag[String]("notmatch")

    val datastream = inputs.connect(broadcaststream)
      .process(new UserDefineBroadcastProcessFunction(tag, bcsd))

    datastream.print("满足条件")
    datastream.getSideOutput(tag).print("不满足")

    env.execute("Window Stream WordCount")

  }
}
//第一个流类型  第二个流类型  输出类型
class UserDefineBroadcastProcessFunction(tag:OutputTag[String],msd:MapStateDescriptor[String,String]) extends BroadcastProcessFunction[String,String,String]{

  //处理正常流 高吞吐 ，通常在改法读取广播状态
  override def processElement(value: String,
                              ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                              out: Collector[String]): Unit = {
    //获取状态 只读
    val readOnlyMapstate = ctx.getBroadcastState(msd)
    if(readOnlyMapstate.contains("rule")){
      val rule=readOnlyMapstate.get("rule")
      if(value.contains(rule)){//将数据写出去
        out.collect(rule+"\t"+value)
      }else{
        ctx.output(tag,rule+"\t"+value)
      }
    }else{//使用Side out将数据输出
        ctx.output(tag,value)
    }
  }
  //处理广播流，通常在这里修改需要广播的状态 低吞吐
  override def processBroadcastElement(value: String,
                                       ctx: BroadcastProcessFunction[String, String, String]#Context,
                                       out: Collector[String]): Unit = {
    val mapstate = ctx.getBroadcastState(msd)
    mapstate.put("rule",value)
  }
}