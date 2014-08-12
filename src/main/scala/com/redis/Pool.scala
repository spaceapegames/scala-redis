package com.redis

import org.apache.commons.pool._

private [redis] class RedisClientFactory(node: RedisNode)
  extends PoolableObjectFactory[RedisClient] {

  // when we make an object it's already connected
  def makeObject = {
    val cl = new RedisClient(node.host, node.port)
    if (node.database != 0)
      cl.select(node.database)
    node.secret.foreach(cl auth _)
    cl
  }

  // quit & disconnect
  def destroyObject(rc: RedisClient): Unit = {
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  def passivateObject(rc: RedisClient): Unit = {}
  def validateObject(rc: RedisClient) = rc.connected == true

  // noop: it should be connected already
  def activateObject(rc: RedisClient): Unit = {}
}
