package com.baizhi.sinks

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class UserDefinedRedisMapper extends RedisMapper[(String,Int)]{
  override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"wordcounts")
  }

  override def getKeyFromData(data: (String, Int)): String = data._1

  override def getValueFromData(data: (String, Int)): String = data._2+""
}
