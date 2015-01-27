package com.redis.sentinel

import com.redis.{RedisNode}
import org.apache.commons.pool.PoolableObjectFactory

private[sentinel] class SentinelClientFactory (node: RedisNode)
  extends PoolableObjectFactory[SentinelClient] {

  // when we make an object it's already connected
  def makeObject = {
    new SentinelClient(node.host, node.port)
  }

  // quit & disconnect
  def destroyObject(rc: SentinelClient): Unit = {
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  def passivateObject(rc: SentinelClient): Unit = {}
  def validateObject(rc: SentinelClient) = rc.connected == true

  // noop: it should be connected already
  def activateObject(rc: SentinelClient): Unit = {}
}

