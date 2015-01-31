package com.redis

trait PoolListener {
  def onMakeObject
  def onDestroyObject
}
