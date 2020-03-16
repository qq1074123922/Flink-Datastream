package com.baizhi.sinks

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class UserDefinedKeyedSerializationSchema  extends KeyedSerializationSchema[(String,Int)] {
  override def serializeKey(element: (String, Int)): Array[Byte] = {
    element._1.getBytes()
  }

  override def serializeValue(element: (String, Int)): Array[Byte] = {
    element._2.toString.getBytes()
  }

  //如果返回null系统会使用defultTopic
  override def getTargetTopic(element: (String, Int)): String = {
    null
  }
}
