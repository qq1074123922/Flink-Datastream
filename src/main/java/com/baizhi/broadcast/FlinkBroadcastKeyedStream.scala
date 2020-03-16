package com.baizhi.broadcast

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{MapStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class OrderItem(id:String,name:String,category:String,count:Int,price:Double)
case class Rule(category:String,threshold:Double)
case class User(id:String,name:String)
object FlinkBroadcastKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //id name 品类 数量 单价   -- 订单项
    //1 zhangsan 水果 2 4.5
    //吞吐量高
    val inputs = env.socketTextStream("CentOS", 9999)
                    .map(line=>line.split("\\s+"))
                    .map(ts=>OrderItem(ts(0),ts(1),ts(2),ts(3).toInt,ts(4).toDouble))
                    .keyBy(orderItem=> orderItem.category+":"+orderItem.id)

    //品类 阈值        水果 8.0       -- 奖  励
    //
   val bcsd=new MapStateDescriptor[String,Double]("bcsd",createTypeInformation[String],createTypeInformation[Double])
    val broadcaststream = env.socketTextStream("CentOS", 8888)
                             .map(line=>line.split("\\s+"))
                             .map(ts=>Rule(ts(0),ts(1).toDouble))
                             .broadcast(bcsd)

    inputs.connect(broadcaststream)
          .process(new UserDefineKeyedBroadcastProcessFunction(bcsd))
          .print("奖励：")

    env.execute("Window Stream WordCount")

  }
}
//key类型 第一个流类型  第二个流类型  输出类型
class UserDefineKeyedBroadcastProcessFunction(msd:MapStateDescriptor[String,Double])
                             extends KeyedBroadcastProcessFunction[String,OrderItem,Rule,User]{

  var userTotalCost:ReducingState[Double]=_

  override def open(parameters: Configuration): Unit = {

    val rsd = new ReducingStateDescriptor[Double]("userTotalCost", new ReduceFunction[Double] {
      override def reduce(value1: Double, value2: Double): Double = value1 + value2
    }, createTypeInformation[Double])

    userTotalCost=getRuntimeContext.getReducingState(rsd)

  }

  override def processElement(value: OrderItem,
                              ctx: KeyedBroadcastProcessFunction[String, OrderItem, Rule, User]#ReadOnlyContext,
                              out: Collector[User]): Unit = {

    //计算出当前品类别下用户的总消费
    userTotalCost.add(value.count*value.price)
    val ruleState = ctx.getBroadcastState(msd)
    var u=User(value.id,value.name)
    //设定的有奖励规则
    if(ruleState!=null && ruleState.contains(value.category)){
      if(userTotalCost.get() >= ruleState.get(value.category)){//达到了奖励阈值
        out.collect(u)
        userTotalCost.clear()
      }else{
        println("不满足条件:"+u+" 当前总消费:"+userTotalCost.get()+" threshold:"+ruleState.get(value.category))
      }
    }
  }

  override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[String, OrderItem, Rule, User]#Context, out: Collector[User]): Unit = {
    val broadcastState = ctx.getBroadcastState(msd)
    broadcastState.put(value.category,value.threshold)
  }
}