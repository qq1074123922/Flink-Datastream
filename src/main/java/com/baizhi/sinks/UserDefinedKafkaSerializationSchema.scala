package com.baizhi.sinks

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class UserDefinedKafkaSerializationSchema extends KafkaSerializationSchema[(String,Int)]{
  override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

    return new ProducerRecord("topic01",element._1.getBytes(),element._2.toString.getBytes())
  }
}
