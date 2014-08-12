package com.redis.sentinel

import com.redis.RedisNode

trait SentinelMonitoredRedisMaster {
  def getMasterName: String
  def onMasterChange (newRedisNode: RedisNode)
  def onMasterHeartBeat (newRedisNode: RedisNode)
}
