package com.redis.sentinel

import com.redis._
import java.util.{TimerTask, Date, Timer}
import com.redis.RedisGenericPoolConfig

class RedisSlavePoolsBySentinel(val masterName: String, sentinelCluster: SentinelCluster, val maxIdle: Int = 8, val database: Int = 0, val secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) {
  private var redisSlaves: List[RedisClientPoolByAddress] = _
  private val timer = new Timer()
  private var roundrobinCounter = 0

  init
  private def init {
    this.synchronized {
      redisSlaves = sentinelCluster.getSlaves(masterName).map {
        redisNode =>
          new RedisClientPoolByAddress(new RedisNode(redisNode.host + ":" + String.valueOf(redisNode.port), redisNode.host, redisNode.port, maxIdle, database, secret), poolConfig)
      }
    }
  }

  def startSlaveSync(interval: Int) {
    val nextStartTime = new Date(System.currentTimeMillis() + interval)
    timer.scheduleAtFixedRate(new slaveHeartbeat, nextStartTime, interval)
  }
  def stopSlaveSync {
    timer.cancel()
  }

  def getNextSlave: Option[RedisClientPoolByAddress] = {
    this.synchronized {
      roundrobinCounter += 1
      if (redisSlaves.size == 0) return None
      else if (redisSlaves.size <= roundrobinCounter){
        roundrobinCounter = 0
      }
      Some(redisSlaves(roundrobinCounter))
    }
  }

  def removeSlave(redisNode: RedisNode){
    this.synchronized {
      val (sameNodes, diffNodes) = redisSlaves.partition(_.getNode == redisNode)
      redisSlaves = diffNodes
      sameNodes
    }.foreach(_.close)
  }

  def close {
    stopSlaveSync
    this.synchronized {
      redisSlaves.foreach(_.close)
      redisSlaves = List.empty
    }
  }

  private class slaveHeartbeat extends TimerTask with Log {
    def run() {
      try {
        val latestNodes = sentinelCluster.getSlaves(masterName).toSet
        val currentNodes = redisSlaves.map(_.getNode).toSet

        //remove no longer exist nodes
        val (removalNodes, newNodes) = this.synchronized {
          val (stillExistNodes, noLongerExistNodes) = redisSlaves.partition(slave => latestNodes.contains(slave.getNode))
          redisSlaves = stillExistNodes
          (noLongerExistNodes, latestNodes.filter(!currentNodes.contains(_)))
        }
        removalNodes.foreach(_.close)

        val newSlaves = newNodes.toList.map{
          redisNode =>
            new RedisClientPoolByAddress(new RedisNode(redisNode.host + ":" + String.valueOf(redisNode.port), redisNode.host, redisNode.port, maxIdle, database, secret), poolConfig)
        }
        //add new nodes
        this.synchronized{
          redisSlaves = redisSlaves.:::(newSlaves)
        }
      } catch {
        case err: Throwable => error("Failed to refresh feature status", err)
      }
    }
  }
}