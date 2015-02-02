package com.redis

trait PoolListener {
  def onMakeObject(node: RedisNode)
  def onDestroyObject(node: RedisNode)
}
