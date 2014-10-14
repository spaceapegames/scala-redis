package com.redis.sentinel

import com.redis.RedisNode

trait SentinelListener {
  def onMasterChange (node: RedisNode)
  def onMastersHeartBeat (values: List[Map[String, String]])
  def heartBeatFailure {}
  def subscriptionFailure {}
  def addNewSentinelNode(addr: SentinelAddress)
  def removeSentinelNode(addr: SentinelAddress)
}
