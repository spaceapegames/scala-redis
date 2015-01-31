package com.redis

import org.apache.commons.pool._

private [redis] class RedisClientFactory(node: RedisNode, listener: Option[PoolListener])
  extends PoolableObjectFactory[RedisClient] {

  def this (node: RedisNode){
    this(node, None)
  }
  // when we make an object it's already connected
  def makeObject = {
    val cl = new RedisClient(node.host, node.port)
    if (node.database != 0)
      cl.select(node.database)
    node.secret.foreach(cl auth _)
    listener.foreach(_.onMakeObject)
    cl
  }

  // quit & disconnect
  def destroyObject(rc: RedisClient): Unit = {
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
    listener.foreach(_.onDestroyObject)
  }

  // noop: we want to have it connected
  def passivateObject(rc: RedisClient): Unit = {}
  def validateObject(rc: RedisClient) = rc.connected == true

  // noop: it should be connected already
  def activateObject(rc: RedisClient): Unit = {}
}
