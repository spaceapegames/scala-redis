package com.redis.sentinel

import com.redis.{Log, RedisNode, RedisMasterNotFoundException, RedisClient}

class RedisClientBySentinel(val masterName: String, sentinelCluster: SentinelCluster, onClientConnected: RedisNode => Unit) extends SentinelMonitoredRedisMaster with Log{
  private var host: String = null
  private var port: Int = 0
  private var client: RedisClient = null

  init

  private def init {
    initMasterNode
    reconnectClient
  }

  private def initMasterNode {
    val redisNode = sentinelCluster.getMasterNode(masterName).getOrElse(throw new RedisMasterNotFoundException(masterName))
    host = redisNode.host
    port = redisNode.port
  }

  def reconnectClient {
    if (client != null) {
      client.disconnect
    }
    val newClient = new RedisClient(host, port)
    client = newClient
  }

  private def updateClient(newRedisNode: RedisNode) {
    if (newRedisNode.host != host || newRedisNode.port != port) {
      ifDebug("redis node address is changing from "+host+":"+port+" to "+newRedisNode.host+":"+newRedisNode.port)
      host = newRedisNode.host
      port = newRedisNode.port
      reconnectClient
      onClientConnected(newRedisNode)
    }
  }

  def getMasterName: String = masterName

  def onMasterChange(newRedisNode: RedisNode) {
    updateClient(newRedisNode)
  }

  def onMasterHeartBeat(newRedisNode: RedisNode) {
    updateClient(newRedisNode)
  }

  def withClient[T](body: RedisClient => T) = {
    body(client)
  }

  def getCurrentClient = client
}
