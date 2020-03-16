package com.baizhi.source

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.api.scala._

class UserDefinedKafkaDeserializationSchema extends KafkaDeserializationSchema[(String,String,Int,Long)]{

  override def isEndOfStream(t: (String, String, Int, Long)): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String, Int, Long) = {
    if(consumerRecord.key()!=null){
      (new String(consumerRecord.key()),new String(consumerRecord.value()),consumerRecord.partition(),consumerRecord.offset())
    }else{
      (null,new String(consumerRecord.value()),consumerRecord.partition(),consumerRecord.offset())
    }
  }

  override def getProducedType: TypeInformation[(String, String, Int, Long)] = {
    createTypeInformation[(String, String, Int, Long)]
  }
}
