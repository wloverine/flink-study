package com.jkl.apitest.sinktest

import com.jkl.apitest.sourcetest.SensorReading
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
