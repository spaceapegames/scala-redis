package com.redis.sentinel

import com.redis.RedisNode

trait SentinelListener {
  def onMasterChange (node: RedisNode)
  def onMastersHeartBeat (values: List[Map[String, String]])
}