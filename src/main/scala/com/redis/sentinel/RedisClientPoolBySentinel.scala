package com.redis.sentinel

import com.redis._
import com.redis.RedisGenericPoolConfig

class RedisClientPoolBySentinel(val masterName: String, val sentinelCluster: SentinelCluster, val maxIdle: Int = 8, val database: Int = 0, val secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) extends RedisClientPool with Log
  with SentinelMonitoredRedisMaster{

  private var pool: RedisClientPoolByAddress = null
  init

  private def init {
    val redisNode = sentinelCluster.getMasterNode(masterName).getOrElse(throw new RedisMasterNotFoundException(masterName))
    pool = new RedisClientPoolByAddress(redisNode.host, redisNode.port, maxIdle, database, secret, poolConfig)
    sentinelCluster.addMonitoredRedisMaster(this)
  }
  def poolName: String = masterName

  def withClient[T](body: RedisClient => T) = {
    pool.withClient(body(_))
  }

  def getNode: RedisNode = pool.getNode

  def getMasterName: String = masterName
  def onMasterChange (newRedisNode: RedisNode) {
    info("master changed %s %s:%s",newRedisNode.name,newRedisNode.host,newRedisNode.port)
    updatePool(getNode.copy(host = newRedisNode.host, port = newRedisNode.port))
  }
  def onMasterHeartBeat (newRedisNode: RedisNode) {
    updatePool(getNode.copy(host = newRedisNode.host, port = newRedisNode.port))
  }

  private def updatePool(redisNode: RedisNode){
    if (pool.isRedisNode(redisNode)){
      return
    }
    info("pool updated %s %s:%s",redisNode.name,redisNode.host,redisNode.port)
    val newPool = new RedisClientPoolByAddress(redisNode.host, redisNode.port, maxIdle, database, secret, poolConfig)
    val oldPool = pool
    pool = newPool
    oldPool.close
  }

  def close {
    sentinelCluster.removeMonitoredRedisMaster(this)
    pool.close
  }
}

trait PoolCreationBySentinel {
  def getSentinelCluster: SentinelCluster

  def poolCreator (node: RedisNode, poolConfig: RedisClientPoolConfig): RedisClientPool = {
    new RedisClientPoolBySentinel(node.name, getSentinelCluster, node.maxIdle, node.database, node.secret, poolConfig)
  }
}
