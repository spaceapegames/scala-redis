package com.redis.sentinel

import com.redis.{Log, RedisNode}
import scala.collection._

class SentinelCluster (clusterConfig: SentinelClusterConfig = SentinelClusterConfig()) extends SentinelListener with Log{
  private var sentinelMonitors = immutable.Map.empty[SentinelAddress, SentinelMonitor]
  private var listeners = Set.empty[SentinelMonitoredRedisMaster]

  def onUpdateSentinels(sentinels: Set[SentinelAddress]){
    sentinels foreach {
      sentinelAddr =>
        addNewSentinelNode(sentinelAddr)
    }
  }

  def addMonitoredRedisMaster(master: SentinelMonitoredRedisMaster) {
    this.synchronized {
      listeners += (master)
    }
  }

  def removeMonitoredRedisMaster(master: SentinelMonitoredRedisMaster){
    this.synchronized {
      listeners -= master
    }
  }

  def onMasterChange (redisNode: RedisNode) {
    this.synchronized {
      listeners.filter(_.getMasterName == redisNode.name)
    }.foreach{
      listener =>
        listener.onMasterChange(redisNode)
    }
  }
  def onMastersHeartBeat (values: List[immutable.Map[String, String]]) {
    values.map(RedisNode(_)).foreach {
      redisNode =>
        this.synchronized {
          listeners.filter(_.getMasterName == redisNode.name)
        }.foreach {
          listener =>
            listener.onMasterHeartBeat(redisNode)
        }
    }
  }

  def getMasterNode(masterName: String): Option[RedisNode] = {
    this.synchronized {
      sentinelMonitors.values
    }.view.map {
        monitor =>
          try {
            if (!monitor.sentinel.connected) {
              monitor.sentinel.reconnect
            }
            //TODO timeout by sentinel request
            monitor.sentinel.master(masterName)
          } catch {
            case e: Exception =>
              warn("failed to get master node %s from sentinel %s:%s", e, masterName, monitor.sentinel.host, monitor.sentinel.port)
              None
          }
      }.find(_.isDefined).getOrElse(None).map(values => {
        RedisNode(values)
      })
  }

  def getSlaves(masterName: String): List[RedisNode] = {
    this.synchronized {
      sentinelMonitors.values
    }.view.map {
      monitor =>
        try {
          if (!monitor.sentinel.connected) {
            monitor.sentinel.reconnect
          }
          monitor.sentinel.slaves(masterName)
        } catch {
          case e: Exception =>
            warn("failed to get master node %s from sentinel %s:%s", e, masterName, monitor.sentinel.host, monitor.sentinel.port)
            None
        }
    }.find(_.isDefined).getOrElse(None).getOrElse(List.empty).filter(_.isDefined).map{
      values =>
        RedisNode(values.get)
    }
  }

  def addNewSentinelNode(addr: SentinelAddress) {
    this.synchronized {
      if (!sentinelMonitors.contains(addr)) {
        try {
          val monitor = new SentinelMonitor(addr, this, clusterConfig)
          sentinelMonitors += (addr -> sentinelMonitors.getOrElse(addr, monitor))
        } catch {
          case e: Throwable =>
            error("failed to start sentinel client at %s:%s", addr.host, addr.port)
        }
      }
    }
  }
  def removeSentinelNode(addr: SentinelAddress) {
    this.synchronized {
      sentinelMonitors.get(addr).foreach {
        sentinel =>
          sentinel.stop
      }
      sentinelMonitors -= addr
    }
  }

  def getStatus: SentinelClusterStatus = {
    this.synchronized {
      SentinelClusterStatus(sentinelMonitors.keySet, listeners.map(entry => {
        (entry.getMasterName -> entry.getNode)
      }).toMap)
    }
  }

  def stopCluster {
    this.synchronized {
      sentinelMonitors.values.foreach(_.stop)
      sentinelMonitors = immutable.Map.empty[SentinelAddress, SentinelMonitor]
    }
  }
}
